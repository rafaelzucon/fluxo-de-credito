package br.com.fluxocredito.motor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import tools.jackson.databind.json.JsonMapper;

class MotorCreditoListenerTest {

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
    private final MotorDecisao motor = mock(MotorDecisao.class);
    private final JsonMapper json = JsonMapper.builder().build();
    private final MotorCreditoListener listener = new MotorCreditoListener(motor, kafka, json, "decisao-tomada");

    private final SolicitacaoCriada solicitacao = new SolicitacaoCriada("e-1", "s-1", "c", "CARTAO-GOLD",
            new BigDecimal("5000"), Instant.parse("2026-11-11T12:00:00Z"));
    private final DecisaoTomada decisao = new DecisaoTomada("e-2", "s-1", "APROVADA", "Cliente elegível",
            Instant.parse("2026-11-11T12:00:01Z"));

    @Test
    void publicaADecisaoComAChaveDaSolicitacao() throws Exception {
        when(motor.decidir(solicitacao)).thenReturn(decisao);
        when(kafka.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        listener.consumir(json.writeValueAsString(solicitacao));

        var payload = ArgumentCaptor.forClass(String.class);
        verify(kafka).send(eq("decisao-tomada"), eq("s-1"), payload.capture());
        assertThat(json.readValue(payload.getValue(), DecisaoTomada.class)).isEqualTo(decisao);
    }

    @Test
    void falhaAoPublicarPropagaParaIrAoRetry() {
        when(motor.decidir(any())).thenReturn(decisao);
        when(kafka.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker")));

        assertThatThrownBy(() -> listener.consumir(json.writeValueAsString(solicitacao)))
                .isInstanceOf(ExecutionException.class);
    }

    @Test
    void payloadInvalidoFalhaSemDecidir() {
        assertThatThrownBy(() -> listener.consumir("{invalido")).isInstanceOf(Exception.class);
        verifyNoInteractions(motor, kafka);
    }
}
