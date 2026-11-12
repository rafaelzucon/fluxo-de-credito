package br.com.fluxocredito.notificacao.aplicacao;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import br.com.fluxocredito.notificacao.dominio.Canal;
import br.com.fluxocredito.notificacao.dominio.DecisaoCredito;
import br.com.fluxocredito.notificacao.infra.idempotencia.RegistroEnvio;

/**
 * Envia a notificação numa única transação: grava a chave de idempotência (UNIQUE) e chama o provedor pelo
 * circuit breaker do canal. Se o envio falhar ou o circuito estiver aberto, o rollback libera a chave para
 * a próxima entrega da mensagem SQS.
 */
@Service
public class ProcessadorNotificacao {

    private static final Logger log = LoggerFactory.getLogger(ProcessadorNotificacao.class);

    private final RegistroEnvio registro;
    private final Map<Canal, EnviadorNotificacao> enviadores = new EnumMap<>(Canal.class);

    public ProcessadorNotificacao(RegistroEnvio registro, List<EnviadorNotificacao> enviadores) {
        this.registro = registro;
        enviadores.forEach(e -> this.enviadores.put(e.canal(), e));
    }

    /** @return {@code true} se enviou; {@code false} se a mensagem era duplicada. */
    @Transactional
    public boolean processar(Canal canal, DecisaoCredito decisao) {
        EnviadorNotificacao enviador = enviadores.get(canal);
        if (enviador == null) {
            throw new IllegalStateException("Nenhum enviador configurado para o canal " + canal);
        }
        if (!registro.registrar(canal, decisao.solicitacaoId())) {
            log.info("{} da solicitação {} já enviado; mensagem duplicada ignorada", canal, decisao.solicitacaoId());
            return false;
        }
        enviador.enviar(decisao);
        log.info("{} da solicitação {} enviado ({})", canal, decisao.solicitacaoId(), decisao.decisao());
        return true;
    }
}
