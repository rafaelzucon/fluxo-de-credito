package br.com.fluxocredito.solicitacao.infra.kafka;

/** Mensagem que não pode ser desserializada; vai direto para o dead letter topic, sem retry. */
public class MensagemInvalidaException extends RuntimeException {

    public MensagemInvalidaException(String mensagem, Throwable causa) {
        super(mensagem, causa);
    }
}
