package br.com.fluxocredito.solicitacao.eventos;

import java.time.Instant;

import br.com.fluxocredito.solicitacao.dominio.Decisao;

/** Evento publicado pelo motor de crédito no tópico decisao-tomada. */
public record DecisaoTomada(
        String eventoId,
        String solicitacaoId,
        Decisao decisao,
        String motivo,
        Instant decididoEm) {
}
