package br.com.fluxocredito.motor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import org.junit.jupiter.api.Test;

class MotorDecisaoTest {

    private static final Instant AGORA = Instant.parse("2026-11-11T12:00:00Z");

    private final ConsultaFatoresElegibilidade fatores = mock(ConsultaFatoresElegibilidade.class);
    private final MotorDecisao motor = new MotorDecisao(fatores, Clock.fixed(AGORA, ZoneOffset.UTC));

    @Test
    void aprovaClienteElegivel() {
        when(fatores.consultar("c")).thenReturn(new FatoresElegibilidade(700, new BigDecimal("10000"), false));

        DecisaoTomada d = motor.decidir(solicitacao("5000"));

        assertThat(d.decisao()).isEqualTo("APROVADA");
        assertThat(d.motivo()).isEqualTo("Cliente elegível");
        assertThat(d.solicitacaoId()).isEqualTo("s-1");
        assertThat(d.decididoEm()).isEqualTo(AGORA);
    }

    @Test
    void valorIgualAoLimiteEAprovado() {
        when(fatores.consultar("c")).thenReturn(new FatoresElegibilidade(MotorDecisao.SCORE_MINIMO,
                new BigDecimal("5000"), false));

        assertThat(motor.decidir(solicitacao("5000.00")).decisao()).isEqualTo("APROVADA");
    }

    @Test
    void restricaoCadastralTemPrioridade() {
        when(fatores.consultar("c")).thenReturn(new FatoresElegibilidade(100, BigDecimal.ONE, true));

        assertNegada(motor.decidir(solicitacao("5000")), "Restrição cadastral");
    }

    @Test
    void scoreAbaixoDoMinimo() {
        when(fatores.consultar("c")).thenReturn(new FatoresElegibilidade(MotorDecisao.SCORE_MINIMO - 1,
                new BigDecimal("10000"), false));

        assertNegada(motor.decidir(solicitacao("5000")), "Score insuficiente");
    }

    @Test
    void valorAcimaDoLimite() {
        when(fatores.consultar("c")).thenReturn(new FatoresElegibilidade(800, new BigDecimal("4999.99"), false));

        assertNegada(motor.decidir(solicitacao("5000")), "Valor acima do limite disponível");
    }

    private static void assertNegada(DecisaoTomada d, String motivo) {
        assertThat(d.decisao()).isEqualTo("NEGADA");
        assertThat(d.motivo()).isEqualTo(motivo);
    }

    private static SolicitacaoCriada solicitacao(String valor) {
        return new SolicitacaoCriada("e-1", "s-1", "c", "CARTAO-GOLD", new BigDecimal(valor), AGORA);
    }
}
