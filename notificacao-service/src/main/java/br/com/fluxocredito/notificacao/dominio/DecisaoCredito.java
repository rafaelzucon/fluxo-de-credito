package br.com.fluxocredito.notificacao.dominio;

import java.math.BigDecimal;
import java.time.Instant;

/** Mensagem publicada no SNS pelo serviço de solicitações e entregue às filas SQS de e-mail e push. */
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
}
