package br.com.fluxocredito.solicitacao.config;

import java.time.Clock;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class Clocks {

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }
}
