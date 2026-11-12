package br.com.fluxocredito.notificacao.infra.email;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.mail.MailSendException;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;

import br.com.fluxocredito.notificacao.Fixtures;
import br.com.fluxocredito.notificacao.config.AppProperties;
import br.com.fluxocredito.notificacao.config.CircuitBreakers;
import br.com.fluxocredito.notificacao.dominio.Decisao;

class EnviadorEmailTest {

    private final JavaMailSender mailSender = mock(JavaMailSender.class);
    private final CircuitBreakerRegistry registry = CircuitBreakerRegistry.ofDefaults();
    private final EnviadorEmail enviador = new EnviadorEmail(mailSender, registry,
            new AppProperties(null, new AppProperties.Email("nao-responda@teste.com"), null, null));

    @Test
    void enviaEmailAoCliente() {
        enviador.enviar(Fixtures.decisao(Decisao.APROVADA));

        var email = ArgumentCaptor.forClass(SimpleMailMessage.class);
        verify(mailSender).send(email.capture());
        assertThat(email.getValue().getFrom()).isEqualTo("nao-responda@teste.com");
        assertThat(email.getValue().getTo()).containsExactly("cliente@exemplo.com");
        assertThat(email.getValue().getSubject()).isEqualTo("Sua solicitação de crédito foi aprovada");
    }

    @Test
    void falhaDoSmtpContaNoCircuito() {
        doThrow(new MailSendException("smtp fora")).when(mailSender).send(any(SimpleMailMessage.class));

        assertThatThrownBy(() -> enviador.enviar(Fixtures.decisao(Decisao.NEGADA)))
                .isInstanceOf(MailSendException.class);
        assertThat(registry.circuitBreaker(CircuitBreakers.EMAIL_PROVIDER).getMetrics().getNumberOfFailedCalls())
                .isEqualTo(1);
    }

    @Test
    void circuitoAbertoNaoChamaOSmtp() {
        registry.circuitBreaker(CircuitBreakers.EMAIL_PROVIDER).transitionToOpenState();

        assertThatThrownBy(() -> enviador.enviar(Fixtures.decisao(Decisao.APROVADA)))
                .isInstanceOf(CallNotPermittedException.class);
        verifyNoInteractions(mailSender);
    }
}
