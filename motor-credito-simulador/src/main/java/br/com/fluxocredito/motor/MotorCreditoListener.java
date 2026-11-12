package br.com.fluxocredito.motor;

import java.time.Clock;

import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.retrytopic.SameIntervalTopicReuseStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/**
 * Consome SolicitacaoCriada, decide e publica DecisaoTomada. Falhas seguem o mesmo padrão do restante da
 * solução: solicitacao-criada-retry (3 tentativas) e depois solicitacao-criada-dlt.
 */
@Component
public class MotorCreditoListener {

    private static final Logger log = LoggerFactory.getLogger(MotorCreditoListener.class);

    private final MotorDecisao motor;
    private final KafkaTemplate<String, String> kafka;
    private final JsonMapper json;
    private final String topicoDecisao;

    public MotorCreditoListener(MotorDecisao motor, KafkaTemplate<String, String> kafka, JsonMapper json,
                                @Value("${app.kafka.topico-decisao-tomada}") String topicoDecisao) {
        this.motor = motor;
        this.kafka = kafka;
        this.json = json;
        this.topicoDecisao = topicoDecisao;
    }

    @RetryableTopic(
            attempts = "4",
            backOff = @BackOff(delayString = "${app.kafka.intervalo-retry:5s}"),
            sameIntervalTopicReuseStrategy = SameIntervalTopicReuseStrategy.SINGLE_TOPIC,
            retryTopicSuffix = "-retry",
            // Quem subir primeiro cria o tópico principal; todos usam o mesmo número de partições.
            numPartitions = "3",
            dltTopicSuffix = "-dlt")
    @KafkaListener(id = "motor-credito", topics = "${app.kafka.topico-solicitacao-criada}")
    public void consumir(String payload) throws Exception {
        SolicitacaoCriada solicitacao = json.readValue(payload, SolicitacaoCriada.class);
        DecisaoTomada decisao = motor.decidir(solicitacao);
        kafka.send(topicoDecisao, decisao.solicitacaoId(), json.writeValueAsString(decisao)).get();
        log.info("Solicitação {} decidida: {} ({})", decisao.solicitacaoId(), decisao.decisao(), decisao.motivo());
    }

    @DltHandler
    public void deadLetter(String payload, @Header(KafkaHeaders.RECEIVED_TOPIC) String topico) {
        log.error("Mensagem enviada para o dead letter topic {}: {}", topico, payload);
    }

    @Configuration(proxyBeanMethods = false)
    static class Config {

        @Bean
        Clock clock() {
            return Clock.systemUTC();
        }

        @Bean
        NewTopic topicoDecisaoTomada(@Value("${app.kafka.topico-decisao-tomada}") String topico) {
            return TopicBuilder.name(topico).partitions(3).replicas(1).build();
        }
    }
}
