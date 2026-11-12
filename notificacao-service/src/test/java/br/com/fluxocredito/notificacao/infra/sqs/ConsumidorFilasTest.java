package br.com.fluxocredito.notificacao.infra.sqs;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;

import io.awspring.cloud.sqs.listener.Visibility;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import br.com.fluxocredito.notificacao.Fixtures;
import br.com.fluxocredito.notificacao.aplicacao.ProcessadorNotificacao;
import br.com.fluxocredito.notificacao.config.AppProperties;
import br.com.fluxocredito.notificacao.dominio.Canal;
import br.com.fluxocredito.notificacao.dominio.Decisao;
import br.com.fluxocredito.notificacao.dominio.DecisaoCredito;

class ConsumidorFilasTest {

    private final ProcessadorNotificacao processador = mock(ProcessadorNotificacao.class);
    private final JsonMapper json = JsonMapper.builder().build();
    private final Visibility visibility = mock(Visibility.class);
    private final ConsumidorFilas consumidor = new ConsumidorFilas(processador, json,
            new AppProperties(null, null, null, Duration.ofSeconds(30)));
    private final DecisaoCredito decisao = Fixtures.decisao(Decisao.APROVADA);

    @Test
    void cadaFilaUsaSeuCanal() {
        String payload = json.writeValueAsString(decisao);

        consumidor.consumirEmail(payload, visibility);
        consumidor.consumirPush(payload, visibility);

        verify(processador).processar(Canal.EMAIL, decisao);
        verify(processador).processar(Canal.PUSH, decisao);
        verifyNoInteractions(visibility);
    }

    @Test
    void circuitoAbertoEstendeOVisibilityTimeoutERelanca() {
        when(processador.processar(Canal.PUSH, decisao)).thenThrow(
                CallNotPermittedException.createCallNotPermittedException(CircuitBreaker.ofDefaults("push-provider")));

        assertThatThrownBy(() -> consumidor.consumirPush(json.writeValueAsString(decisao), visibility))
                .isInstanceOf(CallNotPermittedException.class);
        verify(visibility).changeTo(30);
    }

    @Test
    void outrasFalhasMantemOVisibilityTimeoutDaFila() {
        when(processador.processar(Canal.EMAIL, decisao)).thenThrow(new IllegalStateException("smtp"));

        assertThatThrownBy(() -> consumidor.consumirEmail(json.writeValueAsString(decisao), visibility))
                .isInstanceOf(IllegalStateException.class);
        verify(visibility, never()).changeTo(anyInt());
    }

    @Test
    void payloadInvalido() {
        assertThatThrownBy(() -> consumidor.consumirEmail("nao-e-json", visibility))
                .isInstanceOf(MensagemInvalidaException.class);
        verifyNoInteractions(processador);
    }
}
