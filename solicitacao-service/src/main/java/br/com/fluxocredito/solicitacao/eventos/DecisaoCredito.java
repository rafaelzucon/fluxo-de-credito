package br.com.fluxocredito.solicitacao.eventos;

import java.math.BigDecimal;
import java.time.Instant;

import br.com.fluxocredito.solicitacao.dominio.Decisao;
import br.com.fluxocredito.solicitacao.dominio.Solicitacao;

/** Mensagem publicada no SNS e entregue às filas SQS de e-mail e push. */
public record DecisaoCredito(
        String eventoId,
        String solicitacaoId,
        String clienteId,
        String email,
        String tokenDispositivo,
        String produtoCodigo,
        BigDecimal valorSolicitado,
        Decisao decisao,
        String motivo,
        Instant decididoEm) {

    public static DecisaoCredito de(Solicitacao s, DecisaoTomada d) {
        return new DecisaoCredito(d.eventoId(), s.getId(), s.getClienteId(), s.getEmail(), s.getTokenDispositivo(),
                s.getProdutoCodigo(), s.getValorSolicitado(), d.decisao(), d.motivo(), d.decididoEm());
    }
}
