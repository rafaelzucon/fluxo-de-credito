package br.com.fluxocredito.solicitacao.infra.kafka;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

import br.com.fluxocredito.solicitacao.config.AppProperties;
import br.com.fluxocredito.solicitacao.config.CircuitBreakers;
import br.com.fluxocredito.solicitacao.eventos.SolicitacaoCriada;

/** Publica SolicitacaoCriada no Kafka protegido pelo circuit breaker kafka-producer. */
@Component
public class PublicadorSolicitacaoCriada {

    private final KafkaTemplate<String, String> kafka;
    private final JsonMapper json;
    private final CircuitBreaker circuitBreaker;
    private final String topico;
    private final Duration timeout;

    public PublicadorSolicitacaoCriada(KafkaTemplate<String, String> kafka, JsonMapper json,
                                       CircuitBreakerRegistry registry, AppProperties props) {
        this.kafka = kafka;
        this.json = json;
        this.circuitBreaker = registry.circuitBreaker(CircuitBreakers.KAFKA_PRODUCER);
        this.topico = props.kafka().topicoSolicitacaoCriada();
        this.timeout = props.kafka().timeoutPublicacao();
    }

    /**
     * @throws io.github.resilience4j.circuitbreaker.CallNotPermittedException se o circuito estiver aberto
     * @throws FalhaPublicacaoException se o Kafka não confirmar a publicação dentro do timeout
     */
    public void publicar(SolicitacaoCriada evento) {
        circuitBreaker.executeRunnable(() -> enviar(evento));
    }

    private void enviar(SolicitacaoCriada evento) {
        try {
            kafka.send(topico, evento.solicitacaoId(), json.writeValueAsString(evento))
                    .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FalhaPublicacaoException("Publicação interrompida", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new FalhaPublicacaoException("Falha ao publicar SolicitacaoCriada " + evento.solicitacaoId(), e);
        }
    }
}
