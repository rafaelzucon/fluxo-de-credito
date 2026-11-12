package br.com.fluxocredito.solicitacao.aplicacao;

import java.time.Clock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import br.com.fluxocredito.solicitacao.dominio.Solicitacao;
import br.com.fluxocredito.solicitacao.dominio.SolicitacaoNaoEncontradaException;
import br.com.fluxocredito.solicitacao.dominio.SolicitacaoRepository;
import br.com.fluxocredito.solicitacao.eventos.DecisaoCredito;
import br.com.fluxocredito.solicitacao.eventos.DecisaoTomada;
import br.com.fluxocredito.solicitacao.infra.idempotencia.RegistroIdempotencia;
import br.com.fluxocredito.solicitacao.infra.sns.PublicadorDecisaoSns;

/**
 * Processa a DecisaoTomada numa única transação: grava a chave de idempotência (UNIQUE), atualiza o status
 * e publica no SNS pelo circuit breaker sns-publisher. Se a publicação falhar ou o circuito estiver aberto,
 * o rollback libera a chave para a próxima tentativa.
 */
@Service
public class ProcessadorDecisao {

    private static final Logger log = LoggerFactory.getLogger(ProcessadorDecisao.class);

    private final RegistroIdempotencia idempotencia;
    private final SolicitacaoRepository repository;
    private final PublicadorDecisaoSns publicadorSns;
    private final Clock clock;

    public ProcessadorDecisao(RegistroIdempotencia idempotencia, SolicitacaoRepository repository,
                              PublicadorDecisaoSns publicadorSns, Clock clock) {
        this.idempotencia = idempotencia;
        this.repository = repository;
        this.publicadorSns = publicadorSns;
        this.clock = clock;
    }

    @Transactional
    public void processar(DecisaoTomada decisao) {
        String chave = "decisao-tomada:" + decisao.solicitacaoId();
        if (!idempotencia.registrar(chave)) {
            log.info("Decisão da solicitação {} já processada; mensagem duplicada ignorada", decisao.solicitacaoId());
            return;
        }
        Solicitacao solicitacao = repository.findById(decisao.solicitacaoId())
                .orElseThrow(() -> new SolicitacaoNaoEncontradaException(decisao.solicitacaoId()));
        solicitacao.registrarDecisao(decisao.decisao(), decisao.motivo(), clock.instant());
        repository.saveAndFlush(solicitacao);
        publicadorSns.publicar(DecisaoCredito.de(solicitacao, decisao));
        log.info("Solicitação {} atualizada para {} e decisão publicada no SNS", solicitacao.getId(), solicitacao.getStatus());
    }
}
