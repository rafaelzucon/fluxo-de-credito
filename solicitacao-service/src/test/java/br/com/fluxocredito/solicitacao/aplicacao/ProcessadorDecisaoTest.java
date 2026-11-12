package br.com.fluxocredito.solicitacao.aplicacao;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.Optional;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import br.com.fluxocredito.solicitacao.Fixtures;
import br.com.fluxocredito.solicitacao.dominio.Decisao;
import br.com.fluxocredito.solicitacao.dominio.Solicitacao;
import br.com.fluxocredito.solicitacao.dominio.SolicitacaoNaoEncontradaException;
import br.com.fluxocredito.solicitacao.dominio.SolicitacaoRepository;
import br.com.fluxocredito.solicitacao.dominio.StatusSolicitacao;
import br.com.fluxocredito.solicitacao.eventos.DecisaoCredito;
import br.com.fluxocredito.solicitacao.infra.idempotencia.RegistroIdempotencia;
import br.com.fluxocredito.solicitacao.infra.sns.PublicadorDecisaoSns;

class ProcessadorDecisaoTest {

    private final RegistroIdempotencia idempotencia = mock(RegistroIdempotencia.class);
    private final SolicitacaoRepository repository = mock(SolicitacaoRepository.class);
    private final PublicadorDecisaoSns sns = mock(PublicadorDecisaoSns.class);
    private final ProcessadorDecisao processador = new ProcessadorDecisao(idempotencia, repository, sns, Fixtures.CLOCK);

    @Test
    void atualizaStatusEPublicaNoSns() {
        Solicitacao s = Fixtures.solicitacao();
        when(idempotencia.registrar("decisao-tomada:" + s.getId())).thenReturn(true);
        when(repository.findById(s.getId())).thenReturn(Optional.of(s));

        processador.processar(Fixtures.decisao(s.getId(), Decisao.APROVADA));

        assertThat(s.getStatus()).isEqualTo(StatusSolicitacao.APROVADA);
        verify(repository).saveAndFlush(s);
        var publicada = ArgumentCaptor.forClass(DecisaoCredito.class);
        verify(sns).publicar(publicada.capture());
        assertThat(publicada.getValue().email()).isEqualTo("cliente@exemplo.com");
        assertThat(publicada.getValue().tokenDispositivo()).isEqualTo("token-123");
        assertThat(publicada.getValue().decisao()).isEqualTo(Decisao.APROVADA);
    }

    @Test
    void mensagemDuplicadaEIgnorada() {
        when(idempotencia.registrar(any())).thenReturn(false);

        processador.processar(Fixtures.decisao("s-1", Decisao.NEGADA));

        verifyNoInteractions(repository, sns);
    }

    @Test
    void solicitacaoInexistenteFalhaParaIrAoRetry() {
        when(idempotencia.registrar(any())).thenReturn(true);
        when(repository.findById("s-1")).thenReturn(Optional.empty());

        assertThatThrownBy(() -> processador.processar(Fixtures.decisao("s-1", Decisao.APROVADA)))
                .isInstanceOf(SolicitacaoNaoEncontradaException.class);
        verifyNoInteractions(sns);
    }

    @Test
    void circuitoAbertoPropagaAExcecaoParaORollback() {
        Solicitacao s = Fixtures.solicitacao();
        when(idempotencia.registrar(any())).thenReturn(true);
        when(repository.findById(s.getId())).thenReturn(Optional.of(s));
        doThrow(CallNotPermittedException.createCallNotPermittedException(CircuitBreaker.ofDefaults("sns-publisher")))
                .when(sns).publicar(any());

        assertThatThrownBy(() -> processador.processar(Fixtures.decisao(s.getId(), Decisao.APROVADA)))
                .isInstanceOf(CallNotPermittedException.class);
    }
}
