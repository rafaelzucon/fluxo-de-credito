package br.com.fluxocredito.solicitacao.dominio;

public enum StatusSolicitacao {
    /** Gravada e publicada no Kafka, aguardando a decisão do motor de crédito. */
    PENDENTE,
    /** Gravada, mas a publicação no Kafka falhou ou o circuit breaker estava aberto; o job de republicação vai reenviar. */
    PENDENTE_PUBLICACAO,
    APROVADA,
    NEGADA
}
