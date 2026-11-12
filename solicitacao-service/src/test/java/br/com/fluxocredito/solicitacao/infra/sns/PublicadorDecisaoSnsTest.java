package br.com.fluxocredito.solicitacao.infra.sns;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.SnsException;
import tools.jackson.databind.json.JsonMapper;

import br.com.fluxocredito.solicitacao.Fixtures;
import br.com.fluxocredito.solicitacao.config.AppProperties;
import br.com.fluxocredito.solicitacao.config.CircuitBreakers;
import br.com.fluxocredito.solicitacao.dominio.Decisao;
import br.com.fluxocredito.solicitacao.eventos.DecisaoCredito;

class PublicadorDecisaoSnsTest {

    private static final String ARN = "arn:aws:sns:us-east-1:000000000000:decisao-credito";

    private final SnsClient sns = mock(SnsClient.class);
    private final JsonMapper json = JsonMapper.builder().build();
    private final CircuitBreakerRegistry registry = CircuitBreakerRegistry.ofDefaults();
    private final PublicadorDecisaoSns publicador =
            new PublicadorDecisaoSns(sns, json, registry, new AppProperties(null, new AppProperties.Sns(ARN)));

    private final DecisaoCredito decisao = DecisaoCredito.de(Fixtures.solicitacao(),
            Fixtures.decisao("s-1", Decisao.APROVADA));

    @Test
    void publicaJsonNoTopico() {
        publicador.publicar(decisao);

        var request = ArgumentCaptor.forClass(PublishRequest.class);
        verify(sns).publish(request.capture());
        assertThat(request.getValue().topicArn()).isEqualTo(ARN);
        assertThat(json.readValue(request.getValue().message(), DecisaoCredito.class)).isEqualTo(decisao);
    }

    @Test
    void falhaDoSnsContaNoCircuito() {
        when(sns.publish(any(PublishRequest.class))).thenThrow(SnsException.builder().message("erro").build());

        assertThatThrownBy(() -> publicador.publicar(decisao)).isInstanceOf(SnsException.class);
        assertThat(registry.circuitBreaker(CircuitBreakers.SNS_PUBLISHER).getMetrics().getNumberOfFailedCalls())
                .isEqualTo(1);
    }

    @Test
    void circuitoAbertoFalhaRapido() {
        registry.circuitBreaker(CircuitBreakers.SNS_PUBLISHER).transitionToOpenState();

        assertThatThrownBy(() -> publicador.publicar(decisao)).isInstanceOf(CallNotPermittedException.class);
        verifyNoInteractions(sns);
    }
}
