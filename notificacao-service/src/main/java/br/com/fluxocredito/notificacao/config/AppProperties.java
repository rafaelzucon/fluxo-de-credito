package br.com.fluxocredito.notificacao.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("app")
public record AppProperties(Filas filas, Email email, Push push, Duration visibilidadeCircuitoAberto) {

    public record Filas(String email, String push) {
    }

    public record Email(String remetente) {
    }

    public record Push(String url, Duration timeoutConexao, Duration timeoutLeitura) {
    }
}
