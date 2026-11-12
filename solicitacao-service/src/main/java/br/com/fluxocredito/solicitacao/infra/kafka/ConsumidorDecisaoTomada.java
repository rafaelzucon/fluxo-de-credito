package br.com.fluxocredito.solicitacao.infra.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.SameIntervalTopicReuseStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

import br.com.fluxocredito.solicitacao.aplicacao.ProcessadorDecisao;
import br.com.fluxocredito.solicitacao.eventos.DecisaoTomada;

/**
 * Consome DecisaoTomada. Falhas vão para decisao-tomada-retry e, esgotadas as tentativas, para
 * decisao-tomada-dlt. Com o circuit breaker sns-publisher aberto, o retry é bloqueante (ver KafkaConfig)
 * e não consome tentativas.
 */
@Component
public class ConsumidorDecisaoTomada {

    public static final String LISTENER_ID = "decisao-tomada";

    private static final Logger log = LoggerFactory.getLogger(ConsumidorDecisaoTomada.class);

    private final ProcessadorDecisao processador;
    private final JsonMapper json;

    public ConsumidorDecisaoTomada(ProcessadorDecisao processador, JsonMapper json) {
        this.processador = processador;
        this.json = json;
    }

    @RetryableTopic(
            attempts = "${app.kafka.tentativas:4}",
            backOff = @BackOff(delayString = "${app.kafka.intervalo-retry:5s}"),
            sameIntervalTopicReuseStrategy = SameIntervalTopicReuseStrategy.SINGLE_TOPIC,
            retryTopicSuffix = "-retry",
            // Quem subir primeiro cria o tópico principal; todos usam o mesmo número de partições.
            numPartitions = "3",
            dltTopicSuffix = "-dlt",
            exclude = MensagemInvalidaException.class)
    @KafkaListener(id = LISTENER_ID, idIsGroup = false, groupId = "solicitacao-service",
            topics = "${app.kafka.topico-decisao-tomada}")
    public void consumir(String payload) {
        processador.processar(ler(payload));
    }

    @DltHandler
    public void deadLetter(String payload, @Header(KafkaHeaders.RECEIVED_TOPIC) String topico) {
        log.error("Mensagem enviada para o dead letter topic {}: {}", topico, payload);
    }

    private DecisaoTomada ler(String payload) {
        try {
            return json.readValue(payload, DecisaoTomada.class);
        } catch (JacksonException e) {
            throw new MensagemInvalidaException("DecisaoTomada inválida: " + payload, e);
        }
    }
}
