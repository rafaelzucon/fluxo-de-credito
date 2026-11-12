package br.com.fluxocredito.solicitacao.infra.idempotencia;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;

class RegistroIdempotenciaTest {

    private final JdbcTemplate jdbc = mock(JdbcTemplate.class);
    private final RegistroIdempotencia registro = new RegistroIdempotencia(jdbc);

    @Test
    void chaveNovaRetornaTrue() {
        when(jdbc.update(RegistroIdempotencia.INSERT, "k")).thenReturn(1);

        assertThat(registro.registrar("k")).isTrue();
    }

    @Test
    void violacaoDaConstraintUniqueRetornaFalse() {
        when(jdbc.update(RegistroIdempotencia.INSERT, "k")).thenThrow(new DuplicateKeyException("ORA-00001"));

        assertThat(registro.registrar("k")).isFalse();
    }

    @Test
    void outrosErrosDeBancoSaoPropagados() {
        when(jdbc.update(RegistroIdempotencia.INSERT, "k")).thenThrow(new DataAccessResourceFailureException("down"));

        assertThatThrownBy(() -> registro.registrar("k")).isInstanceOf(DataAccessResourceFailureException.class);
    }
}
