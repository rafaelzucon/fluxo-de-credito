package br.com.fluxocredito.solicitacao.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("app")
public record AppProperties(Kafka kafka, Sns sns) {

    public record Kafka(String topicoSolicitacaoCriada, String topicoDecisaoTomada, Duration timeoutPublicacao) {
    }

    public record Sns(String topicoDecisaoArn) {
    }
}
