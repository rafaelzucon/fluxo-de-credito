package br.com.fluxocredito.notificacao.infra.push;

import java.util.Map;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import br.com.fluxocredito.notificacao.aplicacao.EnviadorNotificacao;
import br.com.fluxocredito.notificacao.config.CircuitBreakers;
import br.com.fluxocredito.notificacao.dominio.Canal;
import br.com.fluxocredito.notificacao.dominio.DecisaoCredito;
import br.com.fluxocredito.notificacao.dominio.MensagemCliente;

@Component
public class EnviadorPush implements EnviadorNotificacao {

    static final String CAMINHO = "/v1/push";

    private final RestClient restClient;
    private final CircuitBreaker circuitBreaker;

    public EnviadorPush(@Qualifier("pushRestClient") RestClient restClient, CircuitBreakerRegistry registry) {
        this.restClient = restClient;
        this.circuitBreaker = registry.circuitBreaker(CircuitBreakers.PUSH_PROVIDER);
    }

    @Override
    public Canal canal() {
        return Canal.PUSH;
    }

    @Override
    public void enviar(DecisaoCredito decisao) {
        MensagemCliente mensagem = MensagemCliente.de(decisao);
        var push = new NotificacaoPush(decisao.tokenDispositivo(), mensagem.titulo(), mensagem.corpo(),
                Map.of("solicitacaoId", decisao.solicitacaoId(), "decisao", decisao.decisao().name()));
        circuitBreaker.executeRunnable(() -> restClient.post()
                .uri(CAMINHO)
                .contentType(MediaType.APPLICATION_JSON)
                .body(push)
                .retrieve()
                .toBodilessEntity());
    }
}
