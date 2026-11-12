package br.com.fluxocredito.motor;

import java.time.Instant;

public record DecisaoTomada(
        String eventoId,
        String solicitacaoId,
        String decisao,
        String motivo,
        Instant decididoEm) {
}
