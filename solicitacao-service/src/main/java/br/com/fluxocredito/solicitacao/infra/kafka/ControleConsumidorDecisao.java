package br.com.fluxocredito.solicitacao.infra.kafka;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import br.com.fluxocredito.solicitacao.config.CircuitBreakers;

/**
 * Pausa os consumers de decisao-tomada (principal e retry) quando o circuit breaker sns-publisher abre e
 * os retoma quando ele passa para MEIO-ABERTO ou FECHADO. O consumer do DLT nunca é pausado.
 */
@Component
public class ControleConsumidorDecisao implements SmartInitializingSingleton {

    private static final Logger log = LoggerFactory.getLogger(ControleConsumidorDecisao.class);

    private final CircuitBreakerRegistry circuitBreakers;
    private final KafkaListenerEndpointRegistry listeners;

    public ControleConsumidorDecisao(CircuitBreakerRegistry circuitBreakers, KafkaListenerEndpointRegistry listeners) {
        this.circuitBreakers = circuitBreakers;
        this.listeners = listeners;
    }

    @Override
    public void afterSingletonsInstantiated() {
        circuitBreakers.circuitBreaker(CircuitBreakers.SNS_PUBLISHER).getEventPublisher()
                .onStateTransition(evento -> reagir(evento.getStateTransition().getToState()));
    }

    void reagir(CircuitBreaker.State novoEstado) {
        switch (novoEstado) {
            case OPEN, FORCED_OPEN -> {
                log.warn("Circuit breaker sns-publisher ABERTO: pausando consumers de decisao-tomada");
                containers().forEach(MessageListenerContainer::pause);
            }
            case HALF_OPEN, CLOSED -> {
                log.info("Circuit breaker sns-publisher {}: retomando consumers de decisao-tomada", novoEstado);
                containers().forEach(MessageListenerContainer::resume);
            }
            default -> {
            }
        }
    }

    private java.util.List<MessageListenerContainer> containers() {
        return listeners.getListenerContainers().stream()
                .filter(c -> c.getListenerId() != null
                        && c.getListenerId().startsWith(ConsumidorDecisaoTomada.LISTENER_ID)
                        && !c.getListenerId().endsWith("-dlt"))
                .toList();
    }
}
