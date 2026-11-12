package br.com.fluxocredito.notificacao.aplicacao;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import org.junit.jupiter.api.Test;

import br.com.fluxocredito.notificacao.Fixtures;
import br.com.fluxocredito.notificacao.dominio.Canal;
import br.com.fluxocredito.notificacao.dominio.Decisao;
import br.com.fluxocredito.notificacao.dominio.DecisaoCredito;
import br.com.fluxocredito.notificacao.infra.idempotencia.RegistroEnvio;

class ProcessadorNotificacaoTest {

    private final RegistroEnvio registro = mock(RegistroEnvio.class);
    private final EnviadorNotificacao email = enviador(Canal.EMAIL);
    private final EnviadorNotificacao push = enviador(Canal.PUSH);
    private final ProcessadorNotificacao processador = new ProcessadorNotificacao(registro, List.of(email, push));
    private final DecisaoCredito decisao = Fixtures.decisao(Decisao.APROVADA);

    @Test
    void enviaPeloCanalCorretoQuandoAChaveENova() {
        when(registro.registrar(Canal.PUSH, "sol-1")).thenReturn(true);

        assertThat(processador.processar(Canal.PUSH, decisao)).isTrue();

        verify(push).enviar(decisao);
        verify(email, never()).enviar(any());
    }

    @Test
    void mensagemDuplicadaNaoReenvia() {
        when(registro.registrar(Canal.EMAIL, "sol-1")).thenReturn(false);

        assertThat(processador.processar(Canal.EMAIL, decisao)).isFalse();

        verify(email, never()).enviar(any());
    }

    @Test
    void circuitoAbertoPropagaParaORollbackLiberarAChave() {
        when(registro.registrar(Canal.EMAIL, "sol-1")).thenReturn(true);
        doThrow(CallNotPermittedException.createCallNotPermittedException(CircuitBreaker.ofDefaults("email-provider")))
                .when(email).enviar(decisao);

        assertThatThrownBy(() -> processador.processar(Canal.EMAIL, decisao))
                .isInstanceOf(CallNotPermittedException.class);
    }

    @Test
    void canalSemEnviadorFalhaSemRegistrarChave() {
        var semPush = new ProcessadorNotificacao(registro, List.of(email));

        assertThatThrownBy(() -> semPush.processar(Canal.PUSH, decisao)).isInstanceOf(IllegalStateException.class);
        verify(registro, never()).registrar(any(), any());
    }

    private static EnviadorNotificacao enviador(Canal canal) {
        EnviadorNotificacao e = mock(EnviadorNotificacao.class);
        when(e.canal()).thenReturn(canal);
        return e;
    }
}
