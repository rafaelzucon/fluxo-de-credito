package br.com.fluxocredito.solicitacao.dominio;

public enum Decisao {
    APROVADA,
    NEGADA;

    public StatusSolicitacao paraStatus() {
        return switch (this) {
            case APROVADA -> StatusSolicitacao.APROVADA;
            case NEGADA -> StatusSolicitacao.NEGADA;
        };
    }
}
