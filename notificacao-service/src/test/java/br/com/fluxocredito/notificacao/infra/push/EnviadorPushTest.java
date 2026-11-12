package br.com.fluxocredito.notificacao.infra.push;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.jsonPath;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withServerError;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestClient;

import br.com.fluxocredito.notificacao.Fixtures;
import br.com.fluxocredito.notificacao.config.CircuitBreakers;
import br.com.fluxocredito.notificacao.dominio.Decisao;

class EnviadorPushTest {

    private final RestClient.Builder builder = RestClient.builder().baseUrl("http://push");
    private final MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
    private final CircuitBreakerRegistry registry = CircuitBreakerRegistry.ofDefaults();
    private final EnviadorPush enviador = new EnviadorPush(builder.build(), registry);

    @Test
    void enviaPushParaODispositivo() {
        server.expect(requestTo("http://push/v1/push"))
                .andExpect(method(HttpMethod.POST))
                .andExpect(jsonPath("$.token").value("token-123"))
                .andExpect(jsonPath("$.titulo").value("Sua solicitação de crédito foi aprovada"))
                .andExpect(jsonPath("$.dados.solicitacaoId").value("sol-1"))
                .andExpect(jsonPath("$.dados.decisao").value("APROVADA"))
                .andRespond(withSuccess());

        enviador.enviar(Fixtures.decisao(Decisao.APROVADA));

        server.verify();
    }

    @Test
    void erro5xxDoProvedorPropaga() {
        server.expect(requestTo("http://push/v1/push")).andRespond(withServerError());

        assertThatThrownBy(() -> enviador.enviar(Fixtures.decisao(Decisao.NEGADA)))
                .isInstanceOf(HttpServerErrorException.class);
    }

    @Test
    void circuitoAbertoNaoChamaOProvedor() {
        registry.circuitBreaker(CircuitBreakers.PUSH_PROVIDER).transitionToOpenState();

        assertThatThrownBy(() -> enviador.enviar(Fixtures.decisao(Decisao.APROVADA)))
                .isInstanceOf(CallNotPermittedException.class);
        server.verify();
    }
}
