package br.com.fluxocredito.motor;

import java.math.BigDecimal;
import java.time.Instant;

public record SolicitacaoCriada(
        String eventoId,
        String solicitacaoId,
        String clienteId,
        String produtoCodigo,
        BigDecimal valorSolicitado,
        Instant criadoEm) {
}
