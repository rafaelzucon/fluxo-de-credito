package br.com.fluxocredito.solicitacao.aplicacao;

import java.time.Clock;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import br.com.fluxocredito.solicitacao.config.CircuitBreakers;
import br.com.fluxocredito.solicitacao.dominio.Solicitacao;
import br.com.fluxocredito.solicitacao.dominio.SolicitacaoRepository;
import br.com.fluxocredito.solicitacao.dominio.StatusSolicitacao;
import br.com.fluxocredito.solicitacao.eventos.SolicitacaoCriada;
import br.com.fluxocredito.solicitacao.infra.kafka.PublicadorSolicitacaoCriada;

/**
 * Fallback do circuit breaker kafka-producer: republica as solicitações PENDENTE_PUBLICACAO quando o
 * circuito não está aberto. Uma publicação duplicada é absorvida pelas chaves de idempotência.
 */
@Component
public class RepublicadorSolicitacoesPendentes {

    private static final Logger log = LoggerFactory.getLogger(RepublicadorSolicitacoesPendentes.class);

    private final SolicitacaoRepository repository;
    private final PublicadorSolicitacaoCriada publicador;
    private final CircuitBreaker circuitBreaker;
    private final Clock clock;

    public RepublicadorSolicitacoesPendentes(SolicitacaoRepository repository, PublicadorSolicitacaoCriada publicador,
                                             CircuitBreakerRegistry registry, Clock clock) {
        this.repository = repository;
        this.publicador = publicador;
        this.circuitBreaker = registry.circuitBreaker(CircuitBreakers.KAFKA_PRODUCER);
        this.clock = clock;
    }

    @Scheduled(fixedDelayString = "${app.republicacao.intervalo:15s}")
    public void republicar() {
        if (circuitBreaker.getState() == CircuitBreaker.State.OPEN) {
            log.debug("Circuit breaker kafka-producer aberto; republicação adiada");
            return;
        }
        for (Solicitacao solicitacao : repository.findTop50ByStatusOrderByCriadoEmAsc(StatusSolicitacao.PENDENTE_PUBLICACAO)) {
            try {
                publicador.publicar(SolicitacaoCriada.de(solicitacao));
                solicitacao.marcarPublicada(clock.instant());
                repository.save(solicitacao);
                log.info("Solicitação {} republicada", solicitacao.getId());
            } catch (CallNotPermittedException e) {
                log.warn("Circuit breaker kafka-producer abriu durante a republicação; retomando no próximo ciclo");
                return;
            } catch (RuntimeException e) {
                log.warn("Falha ao republicar a solicitação {}: {}", solicitacao.getId(), e.getMessage());
            }
        }
    }
}
