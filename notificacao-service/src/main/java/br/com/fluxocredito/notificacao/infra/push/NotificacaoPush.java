package br.com.fluxocredito.notificacao.infra.push;

import java.util.Map;

/** Corpo da requisição ao provedor de push. */
public record NotificacaoPush(String token, String titulo, String mensagem, Map<String, String> dados) {
}
