package br.com.fluxocredito.solicitacao.infra.kafka;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

import br.com.fluxocredito.solicitacao.config.CircuitBreakers;

class ControleConsumidorDecisaoTest {

    private final CircuitBreakerRegistry circuitBreakers = CircuitBreakerRegistry.ofDefaults();
    private final KafkaListenerEndpointRegistry listeners = mock(KafkaListenerEndpointRegistry.class);
    private final MessageListenerContainer principal = container("decisao-tomada");
    private final MessageListenerContainer retry = container("decisao-tomada-retry");
    private final MessageListenerContainer dlt = container("decisao-tomada-dlt");
    private final MessageListenerContainer outro = container("outro");

    @BeforeEach
    void setUp() {
        when(listeners.getListenerContainers()).thenReturn(List.of(principal, retry, dlt, outro));
        new ControleConsumidorDecisao(circuitBreakers, listeners).afterSingletonsInstantiated();
    }

    @Test
    void circuitoAbertoPausaPrincipalERetryMasNaoODlt() {
        circuitBreakers.circuitBreaker(CircuitBreakers.SNS_PUBLISHER).transitionToOpenState();

        verify(principal).pause();
        verify(retry).pause();
        verify(dlt, never()).pause();
        verify(outro, never()).pause();
    }

    @Test
    void meioAbertoRetomaOsConsumers() {
        CircuitBreaker cb = circuitBreakers.circuitBreaker(CircuitBreakers.SNS_PUBLISHER);
        cb.transitionToOpenState();
        cb.transitionToHalfOpenState();

        verify(principal).resume();
        verify(retry).resume();
        verify(dlt, never()).resume();
    }

    private static MessageListenerContainer container(String id) {
        MessageListenerContainer c = mock(MessageListenerContainer.class);
        when(c.getListenerId()).thenReturn(id);
        return c;
    }
}
