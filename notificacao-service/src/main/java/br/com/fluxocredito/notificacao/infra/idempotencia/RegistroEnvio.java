package br.com.fluxocredito.notificacao.infra.idempotencia;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import br.com.fluxocredito.notificacao.dominio.Canal;

/** Registra o envio na tabela envio_notificacao; a constraint UNIQUE da chave impede envio duplicado. */
@Component
public class RegistroEnvio {

    static final String INSERT = "INSERT INTO envio_notificacao (chave, canal, solicitacao_id) VALUES (?, ?, ?)";

    private final JdbcTemplate jdbc;

    public RegistroEnvio(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public static String chave(Canal canal, String solicitacaoId) {
        return canal.name().toLowerCase() + ":" + solicitacaoId;
    }

    /** @return {@code true} se a chave é nova; {@code false} se o envio já foi feito. */
    public boolean registrar(Canal canal, String solicitacaoId) {
        try {
            jdbc.update(INSERT, chave(canal, solicitacaoId), canal.name(), solicitacaoId);
            return true;
        } catch (DuplicateKeyException e) {
            return false;
        }
    }
}
