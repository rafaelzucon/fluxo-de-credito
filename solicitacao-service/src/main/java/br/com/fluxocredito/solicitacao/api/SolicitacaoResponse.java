package br.com.fluxocredito.solicitacao.api;

import java.math.BigDecimal;
import java.time.Instant;

import br.com.fluxocredito.solicitacao.dominio.Solicitacao;
import br.com.fluxocredito.solicitacao.dominio.StatusSolicitacao;

public record SolicitacaoResponse(
        String solicitacaoId,
        String clienteId,
        String produtoCodigo,
        BigDecimal valorSolicitado,
        StatusSolicitacao status,
        String motivoDecisao,
        Instant criadoEm,
        Instant atualizadoEm) {

    public static SolicitacaoResponse de(Solicitacao s) {
        return new SolicitacaoResponse(s.getId(), s.getClienteId(), s.getProdutoCodigo(), s.getValorSolicitado(),
                s.getStatus(), s.getMotivoDecisao(), s.getCriadoEm(), s.getAtualizadoEm());
    }
}
