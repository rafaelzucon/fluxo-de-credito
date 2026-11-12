package br.com.fluxocredito.solicitacao.infra.kafka;

import java.time.Duration;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.retrytopic.RetryTopicConfigurationSupport;
import org.springframework.util.backoff.FixedBackOff;

import br.com.fluxocredito.solicitacao.config.AppProperties;

@Configuration
public class KafkaConfig extends RetryTopicConfigurationSupport {

    private final Duration intervaloCircuitoAberto;

    public KafkaConfig(@Value("${app.kafka.intervalo-circuito-aberto:5s}") Duration intervaloCircuitoAberto) {
        this.intervaloCircuitoAberto = intervaloCircuitoAberto;
    }

    @Bean
    NewTopic topicoSolicitacaoCriada(AppProperties props) {
        return TopicBuilder.name(props.kafka().topicoSolicitacaoCriada()).partitions(3).replicas(1).build();
    }

    @Bean
    NewTopic topicoDecisaoTomada(AppProperties props) {
        return TopicBuilder.name(props.kafka().topicoDecisaoTomada()).partitions(3).replicas(1).build();
    }

    /**
     * Com o circuit breaker aberto, a mensagem não deve ir para o tópico de retry nem gastar tentativas:
     * o error handler faz seek no mesmo offset e tenta de novo após o intervalo, sem limite.
     */
    @Override
    protected void configureBlockingRetries(BlockingRetriesConfigurer blockingRetries) {
        blockingRetries
                .retryOn(CallNotPermittedException.class)
                .backOff(new FixedBackOff(intervaloCircuitoAberto.toMillis(), FixedBackOff.UNLIMITED_ATTEMPTS));
    }
}
