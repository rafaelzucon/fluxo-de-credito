package br.com.fluxocredito.notificacao.config;

/** Nomes das instâncias de circuit breaker configuradas em application.yml (resilience4j.circuitbreaker.instances). */
public final class CircuitBreakers {

    public static final String EMAIL_PROVIDER = "email-provider";
    public static final String PUSH_PROVIDER = "push-provider";

    private CircuitBreakers() {
    }
}
