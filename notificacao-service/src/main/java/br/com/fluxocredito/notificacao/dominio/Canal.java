package br.com.fluxocredito.notificacao.dominio;

import br.com.fluxocredito.notificacao.config.CircuitBreakers;

public enum Canal {
    EMAIL(CircuitBreakers.EMAIL_PROVIDER, "fila-email"),
    PUSH(CircuitBreakers.PUSH_PROVIDER, "fila-push");

    private final String circuitBreaker;
    private final String listenerId;

    Canal(String circuitBreaker, String listenerId) {
        this.circuitBreaker = circuitBreaker;
        this.listenerId = listenerId;
    }

    public String circuitBreaker() {
        return circuitBreaker;
    }

    /** Id do container do listener SQS deste canal. */
    public String listenerId() {
        return listenerId;
    }
}
