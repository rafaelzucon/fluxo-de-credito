package br.com.fluxocredito.solicitacao.eventos;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import br.com.fluxocredito.solicitacao.dominio.Solicitacao;

/** Evento publicado no tópico solicitacao-criada e consumido pelo motor de crédito. */
public record SolicitacaoCriada(
        String eventoId,
        String solicitacaoId,
        String clienteId,
        String produtoCodigo,
        BigDecimal valorSolicitado,
        Instant criadoEm) {

    public static SolicitacaoCriada de(Solicitacao s) {
        return new SolicitacaoCriada(UUID.randomUUID().toString(), s.getId(), s.getClienteId(),
                s.getProdutoCodigo(), s.getValorSolicitado(), s.getCriadoEm());
    }
}
