package br.com.fluxocredito.notificacao.aplicacao;

import br.com.fluxocredito.notificacao.dominio.Canal;
import br.com.fluxocredito.notificacao.dominio.DecisaoCredito;

/** Envia a decisão ao cliente por um canal. Implementações protegem a chamada ao provedor com um circuit breaker. */
public interface EnviadorNotificacao {

    Canal canal();

    /**
     * @throws io.github.resilience4j.circuitbreaker.CallNotPermittedException se o circuito do provedor estiver aberto
     */
    void enviar(DecisaoCredito decisao);
}
