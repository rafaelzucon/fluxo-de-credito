package br.com.fluxocredito.solicitacao.config;

/** Nomes das instâncias de circuit breaker configuradas em application.yml (resilience4j.circuitbreaker.instances). */
public final class CircuitBreakers {

    public static final String KAFKA_PRODUCER = "kafka-producer";
    public static final String SNS_PUBLISHER = "sns-publisher";

    private CircuitBreakers() {
    }
}
