package br.com.fluxocredito.notificacao.dominio;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import br.com.fluxocredito.notificacao.Fixtures;

class MensagemClienteTest {

    @Test
    void aprovada() {
        MensagemCliente m = MensagemCliente.de(Fixtures.decisao(Decisao.APROVADA));

        assertThat(m.titulo()).isEqualTo("Sua solicitação de crédito foi aprovada");
        assertThat(m.corpo()).contains("CARTAO-GOLD", "R$", "5.000,00", "Protocolo: sol-1").doesNotContain("Motivo");
    }

    @Test
    void negadaInformaOMotivo() {
        MensagemCliente m = MensagemCliente.de(Fixtures.decisao(Decisao.NEGADA));

        assertThat(m.titulo()).isEqualTo("Resposta da sua solicitação de crédito");
        assertThat(m.corpo()).contains("não foi aprovada", "Motivo: Score insuficiente", "Protocolo: sol-1");
    }
}
