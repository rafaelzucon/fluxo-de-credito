package br.com.fluxocredito.solicitacao.dominio;

public class SolicitacaoNaoEncontradaException extends RuntimeException {

    public SolicitacaoNaoEncontradaException(String id) {
        super("Solicitação não encontrada: " + id);
    }
}
