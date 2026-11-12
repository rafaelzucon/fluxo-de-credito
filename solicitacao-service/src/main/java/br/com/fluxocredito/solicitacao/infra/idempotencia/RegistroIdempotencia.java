package br.com.fluxocredito.solicitacao.infra.idempotencia;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * Registra chaves de idempotência na tabela chave_idempotencia (constraint UNIQUE). Usa JDBC, e não JPA,
 * para que a violação da constraint não marque a transação JPA como rollback-only.
 */
@Component
public class RegistroIdempotencia {

    static final String INSERT = "INSERT INTO chave_idempotencia (chave) VALUES (?)";

    private final JdbcTemplate jdbc;

    public RegistroIdempotencia(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** @return {@code true} se a chave é nova; {@code false} se a mensagem já foi processada. */
    public boolean registrar(String chave) {
        try {
            jdbc.update(INSERT, chave);
            return true;
        } catch (DuplicateKeyException e) {
            return false;
        }
    }
}
