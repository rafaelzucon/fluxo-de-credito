package br.com.fluxocredito.notificacao.infra.sqs;

public class MensagemInvalidaException extends RuntimeException {

    public MensagemInvalidaException(String mensagem, Throwable causa) {
        super(mensagem, causa);
    }
}
