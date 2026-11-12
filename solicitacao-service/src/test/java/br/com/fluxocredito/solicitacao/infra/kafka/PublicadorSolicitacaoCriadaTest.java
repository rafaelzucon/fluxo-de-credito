package br.com.fluxocredito.solicitacao.infra.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import tools.jackson.databind.json.JsonMapper;

import br.com.fluxocredito.solicitacao.Fixtures;
import br.com.fluxocredito.solicitacao.config.AppProperties;
import br.com.fluxocredito.solicitacao.config.CircuitBreakers;
import br.com.fluxocredito.solicitacao.eventos.SolicitacaoCriada;

class PublicadorSolicitacaoCriadaTest {

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
    private final JsonMapper json = JsonMapper.builder().build();
    private final CircuitBreakerRegistry registry = CircuitBreakerRegistry.ofDefaults();
    private final AppProperties props = new AppProperties(
            new AppProperties.Kafka("solicitacao-criada", "decisao-tomada", Duration.ofSeconds(1)), null);
    private final PublicadorSolicitacaoCriada publicador = new PublicadorSolicitacaoCriada(kafka, json, registry, props);

    @Test
    void publicaComAChaveDaSolicitacao() {
        SolicitacaoCriada evento = SolicitacaoCriada.de(Fixtures.solicitacao());
        when(kafka.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        publicador.publicar(evento);

        var payload = ArgumentCaptor.forClass(String.class);
        verify(kafka).send(eq("solicitacao-criada"), eq(evento.solicitacaoId()), payload.capture());
        assertThat(json.readValue(payload.getValue(), SolicitacaoCriada.class)).isEqualTo(evento);
    }

    @Test
    void falhaNoBrokerViraFalhaPublicacaoEContaNoCircuito() {
        when(kafka.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker indisponível")));

        assertThatThrownBy(() -> publicador.publicar(SolicitacaoCriada.de(Fixtures.solicitacao())))
                .isInstanceOf(FalhaPublicacaoException.class);
        assertThat(registry.circuitBreaker(CircuitBreakers.KAFKA_PRODUCER).getMetrics().getNumberOfFailedCalls())
                .isEqualTo(1);
    }

    @Test
    void circuitoAbertoFalhaRapidoSemChamarOKafka() {
        registry.circuitBreaker(CircuitBreakers.KAFKA_PRODUCER).transitionToOpenState();

        assertThatThrownBy(() -> publicador.publicar(SolicitacaoCriada.de(Fixtures.solicitacao())))
                .isInstanceOf(CallNotPermittedException.class);
        verifyNoInteractions(kafka);
    }

    @Test
    void timeoutViraFalhaPublicacao() {
        when(kafka.send(anyString(), anyString(), any())).thenReturn(new CompletableFuture<>());
        var rapido = new PublicadorSolicitacaoCriada(kafka, json, registry, new AppProperties(
                new AppProperties.Kafka("solicitacao-criada", "decisao-tomada", Duration.ofMillis(10)), null));

        assertThatThrownBy(() -> rapido.publicar(SolicitacaoCriada.de(Fixtures.solicitacao())))
                .isInstanceOf(FalhaPublicacaoException.class);
    }
}
