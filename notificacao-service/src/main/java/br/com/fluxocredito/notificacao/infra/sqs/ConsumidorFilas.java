package br.com.fluxocredito.notificacao.infra.sqs;

import io.awspring.cloud.sqs.annotation.SqsListener;
import io.awspring.cloud.sqs.listener.Visibility;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

import br.com.fluxocredito.notificacao.aplicacao.ProcessadorNotificacao;
import br.com.fluxocredito.notificacao.config.AppProperties;
import br.com.fluxocredito.notificacao.dominio.Canal;
import br.com.fluxocredito.notificacao.dominio.DecisaoCredito;

/**
 * Consome as filas de e-mail e push. A mensagem só é apagada se o processamento terminar com sucesso
 * (acknowledgement ON_SUCCESS). Em caso de falha, ela reaparece quando o visibility timeout expira e,
 * depois de 3 recebimentos, a redrive policy a move para a DLQ.
 */
@Component
public class ConsumidorFilas {

    private static final Logger log = LoggerFactory.getLogger(ConsumidorFilas.class);

    private final ProcessadorNotificacao processador;
    private final JsonMapper json;
    private final int visibilidadeCircuitoAberto;

    public ConsumidorFilas(ProcessadorNotificacao processador, JsonMapper json, AppProperties props) {
        this.processador = processador;
        this.json = json;
        this.visibilidadeCircuitoAberto = Math.toIntExact(props.visibilidadeCircuitoAberto().toSeconds());
    }

    @SqsListener(id = "fila-email", queueNames = "${app.filas.email}")
    public void consumirEmail(String payload, Visibility visibility) {
        consumir(Canal.EMAIL, payload, visibility);
    }

    @SqsListener(id = "fila-push", queueNames = "${app.filas.push}")
    public void consumirPush(String payload, Visibility visibility) {
        consumir(Canal.PUSH, payload, visibility);
    }

    void consumir(Canal canal, String payload, Visibility visibility) {
        DecisaoCredito decisao = ler(payload);
        try {
            processador.processar(canal, decisao);
        } catch (CallNotPermittedException e) {
            // A mensagem fica invisível pelo tempo em que o circuito permanece aberto; ControleListeners para o
            // listener e o reinicia quando o circuito passar para MEIO-ABERTO.
            log.warn("Circuit breaker {} aberto; solicitação {} será reentregue em {}s",
                    canal.circuitBreaker(), decisao.solicitacaoId(), visibilidadeCircuitoAberto);
            visibility.changeTo(visibilidadeCircuitoAberto);
            throw e;
        }
    }

    private DecisaoCredito ler(String payload) {
        try {
            return json.readValue(payload, DecisaoCredito.class);
        } catch (JacksonException e) {
            throw new MensagemInvalidaException("DecisaoCredito inválida: " + payload, e);
        }
    }
}
