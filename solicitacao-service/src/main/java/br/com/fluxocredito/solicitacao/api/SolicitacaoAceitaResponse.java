package br.com.fluxocredito.solicitacao.api;

public record SolicitacaoAceitaResponse(String solicitacaoId, String mensagem) {

    public static final String MENSAGEM_PADRAO =
            "Recebemos sua solicitação de crédito. Em breve você receberá a resposta por e-mail e por notificação no app.";

    public static SolicitacaoAceitaResponse para(String solicitacaoId) {
        return new SolicitacaoAceitaResponse(solicitacaoId, MENSAGEM_PADRAO);
    }
}
