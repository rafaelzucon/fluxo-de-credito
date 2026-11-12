package br.com.fluxocredito.notificacao.dominio;

import java.text.NumberFormat;
import java.util.Locale;

/** Texto enviado ao cliente por e-mail e push. */
public record MensagemCliente(String titulo, String corpo) {

    private static final Locale PT_BR = Locale.of("pt", "BR");

    public static MensagemCliente de(DecisaoCredito d) {
        String valor = NumberFormat.getCurrencyInstance(PT_BR).format(d.valorSolicitado());
        return switch (d.decisao()) {
            case APROVADA -> new MensagemCliente(
                    "Sua solicitação de crédito foi aprovada",
                    "Boa notícia! Sua solicitação do produto %s no valor de %s foi aprovada. Protocolo: %s."
                            .formatted(d.produtoCodigo(), valor, d.solicitacaoId()));
            case NEGADA -> new MensagemCliente(
                    "Resposta da sua solicitação de crédito",
                    "Sua solicitação do produto %s no valor de %s não foi aprovada neste momento. Motivo: %s. Protocolo: %s."
                            .formatted(d.produtoCodigo(), valor, d.motivo(), d.solicitacaoId()));
        };
    }
}
