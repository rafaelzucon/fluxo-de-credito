package br.com.fluxocredito.solicitacao.infra.kafka;

public class FalhaPublicacaoException extends RuntimeException {

    public FalhaPublicacaoException(String mensagem, Throwable causa) {
        super(mensagem, causa);
    }
}
