package br.com.fluxocredito.solicitacao.aplicacao;

import java.time.Clock;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import br.com.fluxocredito.solicitacao.api.NovaSolicitacaoRequest;
import br.com.fluxocredito.solicitacao.dominio.Solicitacao;
import br.com.fluxocredito.solicitacao.dominio.SolicitacaoNaoEncontradaException;
import br.com.fluxocredito.solicitacao.dominio.SolicitacaoRepository;
import br.com.fluxocredito.solicitacao.eventos.SolicitacaoCriada;
import br.com.fluxocredito.solicitacao.infra.kafka.PublicadorSolicitacaoCriada;

/**
 * Grava a solicitação e publica SolicitacaoCriada. A gravação é confirmada antes da publicação para que a
 * decisão nunca chegue antes da solicitação existir no banco. Se a publicação falhar ou o circuit breaker
 * kafka-producer estiver aberto, a solicitação fica PENDENTE_PUBLICACAO e o cliente recebe a mesma resposta.
 */
@Service
public class SolicitacaoService {

    private static final Logger log = LoggerFactory.getLogger(SolicitacaoService.class);

    private final SolicitacaoRepository repository;
    private final PublicadorSolicitacaoCriada publicador;
    private final Clock clock;

    public SolicitacaoService(SolicitacaoRepository repository, PublicadorSolicitacaoCriada publicador, Clock clock) {
        this.repository = repository;
        this.publicador = publicador;
        this.clock = clock;
    }

    public Solicitacao criar(NovaSolicitacaoRequest request) {
        Solicitacao solicitacao = repository.save(Solicitacao.nova(request.clienteId(), request.produtoCodigo(),
                request.valorSolicitado(), request.email(), request.tokenDispositivo(), clock.instant()));
        try {
            publicador.publicar(SolicitacaoCriada.de(solicitacao));
            log.info("Solicitação {} criada e publicada", solicitacao.getId());
        } catch (CallNotPermittedException e) {
            log.warn("Circuit breaker kafka-producer aberto; solicitação {} ficará pendente de publicação",
                    solicitacao.getId());
            solicitacao = marcarPendentePublicacao(solicitacao);
        } catch (RuntimeException e) {
            log.error("Falha ao publicar a solicitação {}; ficará pendente de publicação", solicitacao.getId(), e);
            solicitacao = marcarPendentePublicacao(solicitacao);
        }
        return solicitacao;
    }

    public Solicitacao consultar(String id) {
        return repository.findById(id).orElseThrow(() -> new SolicitacaoNaoEncontradaException(id));
    }

    private Solicitacao marcarPendentePublicacao(Solicitacao solicitacao) {
        solicitacao.marcarPendentePublicacao(clock.instant());
        return repository.save(solicitacao);
    }
}
