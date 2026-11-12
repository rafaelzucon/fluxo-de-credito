package br.com.fluxocredito.notificacao.infra.push;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import br.com.fluxocredito.notificacao.config.AppProperties;

@Configuration(proxyBeanMethods = false)
public class PushConfig {

    @Bean
    RestClient pushRestClient(RestClient.Builder builder, AppProperties props) {
        var requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(props.push().timeoutConexao());
        requestFactory.setReadTimeout(props.push().timeoutLeitura());
        return builder.baseUrl(props.push().url()).requestFactory(requestFactory).build();
    }
}
