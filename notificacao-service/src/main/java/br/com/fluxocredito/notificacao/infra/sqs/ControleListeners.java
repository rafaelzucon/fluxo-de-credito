package br.com.fluxocredito.notificacao.infra.sqs;

import io.awspring.cloud.sqs.listener.MessageListenerContainer;
import io.awspring.cloud.sqs.listener.MessageListenerContainerRegistry;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.stereotype.Component;

import br.com.fluxocredito.notificacao.dominio.Canal;

/**
 * Para o listener SQS de um canal quando o circuit breaker do seu provedor abre e o reinicia quando o
 * circuito passa para MEIO-ABERTO ou FECHADO. Cada canal é controlado de forma independente.
 */
@Component
public class ControleListeners implements SmartInitializingSingleton {

    private static final Logger log = LoggerFactory.getLogger(ControleListeners.class);

    private final CircuitBreakerRegistry circuitBreakers;
    private final MessageListenerContainerRegistry listeners;

    public ControleListeners(CircuitBreakerRegistry circuitBreakers, MessageListenerContainerRegistry listeners) {
        this.circuitBreakers = circuitBreakers;
        this.listeners = listeners;
    }

    @Override
    public void afterSingletonsInstantiated() {
        for (Canal canal : Canal.values()) {
            circuitBreakers.circuitBreaker(canal.circuitBreaker()).getEventPublisher()
                    .onStateTransition(evento -> reagir(canal, evento.getStateTransition().getToState()));
        }
    }

    void reagir(Canal canal, CircuitBreaker.State novoEstado) {
        MessageListenerContainer<?> container = listeners.getContainerById(canal.listenerId());
        if (container == null) {
            return;
        }
        // O evento é disparado na thread do listener; parar o container nela mesma causaria deadlock.
        switch (novoEstado) {
            case OPEN, FORCED_OPEN -> {
                log.warn("Circuit breaker {} ABERTO: parando o listener {}", canal.circuitBreaker(), canal.listenerId());
                Thread.ofVirtual().name("stop-" + canal.listenerId()).start(() -> {
                    if (container.isRunning()) {
                        container.stop();
                    }
                });
            }
            case HALF_OPEN, CLOSED -> {
                log.info("Circuit breaker {} {}: iniciando o listener {}", canal.circuitBreaker(), novoEstado,
                        canal.listenerId());
                Thread.ofVirtual().name("start-" + canal.listenerId()).start(() -> {
                    if (!container.isRunning()) {
                        container.start();
                    }
                });
            }
            default -> {
            }
        }
    }
}
