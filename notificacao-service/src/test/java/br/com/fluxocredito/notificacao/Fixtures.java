package br.com.fluxocredito.notificacao;

import java.math.BigDecimal;
import java.time.Instant;

import br.com.fluxocredito.notificacao.dominio.Decisao;
import br.com.fluxocredito.notificacao.dominio.DecisaoCredito;

public final class Fixtures {

    private Fixtures() {
    }

    public static DecisaoCredito decisao(Decisao decisao) {
        return new DecisaoCredito("evt-1", "sol-1", "cliente-1", "cliente@exemplo.com", "token-123",
                "CARTAO-GOLD", new BigDecimal("5000.00"), decisao,
                decisao == Decisao.APROVADA ? "Cliente elegível" : "Score insuficiente",
                Instant.parse("2026-11-11T12:00:00Z"));
    }
}
