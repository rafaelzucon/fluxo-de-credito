package br.com.fluxocredito.solicitacao.infra.sns;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import tools.jackson.databind.json.JsonMapper;

import br.com.fluxocredito.solicitacao.config.AppProperties;
import br.com.fluxocredito.solicitacao.config.CircuitBreakers;
import br.com.fluxocredito.solicitacao.eventos.DecisaoCredito;

/** Publica a decisão no tópico SNS (fan-out para as filas de e-mail e push) pelo circuit breaker sns-publisher. */
@Component
public class PublicadorDecisaoSns {

    private final SnsClient sns;
    private final JsonMapper json;
    private final CircuitBreaker circuitBreaker;
    private final String topicoArn;

    public PublicadorDecisaoSns(SnsClient sns, JsonMapper json, CircuitBreakerRegistry registry, AppProperties props) {
        this.sns = sns;
        this.json = json;
        this.circuitBreaker = registry.circuitBreaker(CircuitBreakers.SNS_PUBLISHER);
        this.topicoArn = props.sns().topicoDecisaoArn();
    }

    public void publicar(DecisaoCredito decisao) {
        String mensagem = json.writeValueAsString(decisao);
        circuitBreaker.executeRunnable(() -> sns.publish(PublishRequest.builder()
                .topicArn(topicoArn)
                .message(mensagem)
                .build()));
    }
}
