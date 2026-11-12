package br.com.fluxocredito.notificacao.infra.email;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Component;

import br.com.fluxocredito.notificacao.aplicacao.EnviadorNotificacao;
import br.com.fluxocredito.notificacao.config.AppProperties;
import br.com.fluxocredito.notificacao.config.CircuitBreakers;
import br.com.fluxocredito.notificacao.dominio.Canal;
import br.com.fluxocredito.notificacao.dominio.DecisaoCredito;
import br.com.fluxocredito.notificacao.dominio.MensagemCliente;

@Component
public class EnviadorEmail implements EnviadorNotificacao {

    private final JavaMailSender mailSender;
    private final CircuitBreaker circuitBreaker;
    private final String remetente;

    public EnviadorEmail(JavaMailSender mailSender, CircuitBreakerRegistry registry, AppProperties props) {
        this.mailSender = mailSender;
        this.circuitBreaker = registry.circuitBreaker(CircuitBreakers.EMAIL_PROVIDER);
        this.remetente = props.email().remetente();
    }

    @Override
    public Canal canal() {
        return Canal.EMAIL;
    }

    @Override
    public void enviar(DecisaoCredito decisao) {
        MensagemCliente mensagem = MensagemCliente.de(decisao);
        var email = new SimpleMailMessage();
        email.setFrom(remetente);
        email.setTo(decisao.email());
        email.setSubject(mensagem.titulo());
        email.setText(mensagem.corpo());
        circuitBreaker.executeRunnable(() -> mailSender.send(email));
    }
}
