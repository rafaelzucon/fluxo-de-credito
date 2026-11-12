package br.com.fluxocredito.solicitacao.aplicacao;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.List;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.junit.jupiter.api.Test;

import br.com.fluxocredito.solicitacao.Fixtures;
import br.com.fluxocredito.solicitacao.config.CircuitBreakers;
import br.com.fluxocredito.solicitacao.dominio.Solicitacao;
import br.com.fluxocredito.solicitacao.dominio.SolicitacaoRepository;
import br.com.fluxocredito.solicitacao.dominio.StatusSolicitacao;
import br.com.fluxocredito.solicitacao.infra.kafka.FalhaPublicacaoException;
import br.com.fluxocredito.solicitacao.infra.kafka.PublicadorSolicitacaoCriada;

class RepublicadorSolicitacoesPendentesTest {

    private final SolicitacaoRepository repository = mock(SolicitacaoRepository.class);
    private final PublicadorSolicitacaoCriada publicador = mock(PublicadorSolicitacaoCriada.class);
    private final CircuitBreakerRegistry registry = CircuitBreakerRegistry.ofDefaults();
    private final RepublicadorSolicitacoesPendentes republicador =
            new RepublicadorSolicitacoesPendentes(repository, publicador, registry, Fixtures.CLOCK);

    @Test
    void naoFazNadaComCircuitoAberto() {
        registry.circuitBreaker(CircuitBreakers.KAFKA_PRODUCER).transitionToOpenState();

        republicador.republicar();

        verifyNoInteractions(repository, publicador);
    }

    @Test
    void republicaEVoltaParaPendente() {
        Solicitacao s = pendentePublicacao();
        when(repository.findTop50ByStatusOrderByCriadoEmAsc(StatusSolicitacao.PENDENTE_PUBLICACAO)).thenReturn(List.of(s));

        republicador.republicar();

        verify(publicador).publicar(any());
        verify(repository).save(s);
        assertThat(s.getStatus()).isEqualTo(StatusSolicitacao.PENDENTE);
    }

    @Test
    void falhaEmUmaNaoImpedeAsDemais() {
        Solicitacao falha = pendentePublicacao();
        Solicitacao ok = pendentePublicacao();
        when(repository.findTop50ByStatusOrderByCriadoEmAsc(StatusSolicitacao.PENDENTE_PUBLICACAO))
                .thenReturn(List.of(falha, ok));
        doThrow(new FalhaPublicacaoException("erro", null)).doNothing().when(publicador).publicar(any());

        republicador.republicar();

        assertThat(falha.getStatus()).isEqualTo(StatusSolicitacao.PENDENTE_PUBLICACAO);
        assertThat(ok.getStatus()).isEqualTo(StatusSolicitacao.PENDENTE);
        verify(repository, never()).save(falha);
    }

    @Test
    void paraNoCicloQuandoOCircuitoAbre() {
        when(repository.findTop50ByStatusOrderByCriadoEmAsc(StatusSolicitacao.PENDENTE_PUBLICACAO))
                .thenReturn(List.of(pendentePublicacao(), pendentePublicacao()));
        doThrow(CallNotPermittedException.createCallNotPermittedException(
                registry.circuitBreaker(CircuitBreakers.KAFKA_PRODUCER))).when(publicador).publicar(any());

        republicador.republicar();

        verify(publicador, times(1)).publicar(any());
        verify(repository, never()).save(any());
    }

    private static Solicitacao pendentePublicacao() {
        Solicitacao s = Fixtures.solicitacao();
        s.marcarPendentePublicacao(Fixtures.AGORA);
        return s;
    }
}
