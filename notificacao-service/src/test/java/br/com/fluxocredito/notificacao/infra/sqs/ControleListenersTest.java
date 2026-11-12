package br.com.fluxocredito.notificacao.infra.sqs;

import static org.mockito.Mockito.after;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.awspring.cloud.sqs.listener.MessageListenerContainer;
import io.awspring.cloud.sqs.listener.MessageListenerContainerRegistry;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import br.com.fluxocredito.notificacao.config.CircuitBreakers;

class ControleListenersTest {

    private final CircuitBreakerRegistry circuitBreakers = CircuitBreakerRegistry.ofDefaults();
    private final MessageListenerContainerRegistry listeners = mock(MessageListenerContainerRegistry.class);
    private final MessageListenerContainer<?> email = mock(MessageListenerContainer.class);
    private final MessageListenerContainer<?> push = mock(MessageListenerContainer.class);

    @BeforeEach
    void setUp() {
        when(listeners.getContainerById("fila-email")).thenAnswer(i -> email);
        when(listeners.getContainerById("fila-push")).thenAnswer(i -> push);
        new ControleListeners(circuitBreakers, listeners).afterSingletonsInstantiated();
    }

    @Test
    void circuitoAbertoParaApenasOListenerDoCanal() {
        when(email.isRunning()).thenReturn(true);
        when(push.isRunning()).thenReturn(true);

        circuitBreakers.circuitBreaker(CircuitBreakers.EMAIL_PROVIDER).transitionToOpenState();

        verify(email, timeout(1000)).stop();
        verify(push, after(200).never()).stop();
    }

    @Test
    void meioAbertoReiniciaOListener() {
        CircuitBreaker cb = circuitBreakers.circuitBreaker(CircuitBreakers.PUSH_PROVIDER);
        cb.transitionToOpenState();
        when(push.isRunning()).thenReturn(false);

        cb.transitionToHalfOpenState();

        verify(push, timeout(1000)).start();
    }
}
