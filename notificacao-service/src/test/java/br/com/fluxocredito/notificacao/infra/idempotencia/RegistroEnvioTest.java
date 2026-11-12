package br.com.fluxocredito.notificacao.infra.idempotencia;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;

import br.com.fluxocredito.notificacao.dominio.Canal;

class RegistroEnvioTest {

    private final JdbcTemplate jdbc = mock(JdbcTemplate.class);
    private final RegistroEnvio registro = new RegistroEnvio(jdbc);

    @Test
    void chaveNovaGravaPorCanalESolicitacao() {
        assertThat(registro.registrar(Canal.EMAIL, "s-1")).isTrue();
        verify(jdbc).update(RegistroEnvio.INSERT, "email:s-1", "EMAIL", "s-1");
    }

    @Test
    void chaveDuplicadaRetornaFalse() {
        when(jdbc.update(RegistroEnvio.INSERT, "push:s-1", "PUSH", "s-1"))
                .thenThrow(new DuplicateKeyException("ORA-00001"));

        assertThat(registro.registrar(Canal.PUSH, "s-1")).isFalse();
    }
}
