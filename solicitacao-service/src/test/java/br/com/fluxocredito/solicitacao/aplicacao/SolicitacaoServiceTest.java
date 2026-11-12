package br.com.fluxocredito.solicitacao.aplicacao;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import br.com.fluxocredito.solicitacao.Fixtures;
import br.com.fluxocredito.solicitacao.dominio.Solicitacao;
import br.com.fluxocredito.solicitacao.dominio.SolicitacaoNaoEncontradaException;
import br.com.fluxocredito.solicitacao.dominio.SolicitacaoRepository;
import br.com.fluxocredito.solicitacao.dominio.StatusSolicitacao;
import br.com.fluxocredito.solicitacao.eventos.SolicitacaoCriada;
import br.com.fluxocredito.solicitacao.infra.kafka.FalhaPublicacaoException;
import br.com.fluxocredito.solicitacao.infra.kafka.PublicadorSolicitacaoCriada;

class SolicitacaoServiceTest {

    private final SolicitacaoRepository repository = mock(SolicitacaoRepository.class);
    private final PublicadorSolicitacaoCriada publicador = mock(PublicadorSolicitacaoCriada.class);
    private final SolicitacaoService service = new SolicitacaoService(repository, publicador, Fixtures.CLOCK);

    @BeforeEach
    void setUp() {
        when(repository.save(any())).thenAnswer(i -> i.getArgument(0));
    }

    @Test
    void gravaAntesDePublicarEMantemPendente() {
        Solicitacao s = service.criar(Fixtures.request());

        var evento = ArgumentCaptor.forClass(SolicitacaoCriada.class);
        var ordem = org.mockito.Mockito.inOrder(repository, publicador);
        ordem.verify(repository).save(any());
        ordem.verify(publicador).publicar(evento.capture());
        assertThat(evento.getValue().solicitacaoId()).isEqualTo(s.getId());
        assertThat(evento.getValue().clienteId()).isEqualTo("cliente-1");
        assertThat(s.getStatus()).isEqualTo(StatusSolicitacao.PENDENTE);
    }

    @Test
    void circuitoAbertoDeixaPendentePublicacao() {
        doThrow(CallNotPermittedException.createCallNotPermittedException(CircuitBreaker.ofDefaults("kafka-producer")))
                .when(publicador).publicar(any());

        Solicitacao s = service.criar(Fixtures.request());

        assertThat(s.getStatus()).isEqualTo(StatusSolicitacao.PENDENTE_PUBLICACAO);
        verify(repository, times(2)).save(s);
    }

    @Test
    void falhaNaPublicacaoDeixaPendentePublicacao() {
        doThrow(new FalhaPublicacaoException("timeout", null)).when(publicador).publicar(any());

        Solicitacao s = service.criar(Fixtures.request());

        assertThat(s.getStatus()).isEqualTo(StatusSolicitacao.PENDENTE_PUBLICACAO);
    }

    @Test
    void consultaInexistenteLancaExcecao() {
        when(repository.findById("x")).thenReturn(Optional.empty());

        assertThatThrownBy(() -> service.consultar("x")).isInstanceOf(SolicitacaoNaoEncontradaException.class);
    }
}
