package br.com.fluxocredito.motor;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;

import org.junit.jupiter.api.Test;

class ConsultaFatoresElegibilidadeTest {

    private final ConsultaFatoresElegibilidade consulta = new ConsultaFatoresElegibilidade();

    @Test
    void clienteRestritoTemRestricaoCadastral() {
        assertThat(consulta.consultar("RESTRITO-42").restricaoCadastral()).isTrue();
        assertThat(consulta.consultar("cliente-42").restricaoCadastral()).isFalse();
    }

    @Test
    void fatoresSaoDeterministicosEDentroDasFaixas() {
        FatoresElegibilidade f = consulta.consultar("cliente-1");

        assertThat(consulta.consultar("cliente-1")).isEqualTo(f);
        assertThat(f.score()).isBetween(300, 1000);
        assertThat(f.limiteDisponivel()).isBetween(new BigDecimal("1000"), new BigDecimal("50000"));
    }
}
