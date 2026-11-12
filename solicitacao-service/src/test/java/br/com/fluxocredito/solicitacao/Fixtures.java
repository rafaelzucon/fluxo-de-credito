package br.com.fluxocredito.solicitacao;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import br.com.fluxocredito.solicitacao.api.NovaSolicitacaoRequest;
import br.com.fluxocredito.solicitacao.dominio.Decisao;
import br.com.fluxocredito.solicitacao.dominio.Solicitacao;
import br.com.fluxocredito.solicitacao.eventos.DecisaoTomada;

public final class Fixtures {

    public static final Instant AGORA = Instant.parse("2026-11-11T12:00:00Z");
    public static final Clock CLOCK = Clock.fixed(AGORA, ZoneOffset.UTC);

    private Fixtures() {
    }

    public static NovaSolicitacaoRequest request() {
        return new NovaSolicitacaoRequest("cliente-1", "CARTAO-GOLD", new BigDecimal("5000.00"),
                "cliente@exemplo.com", "token-123");
    }

    public static Solicitacao solicitacao() {
        return Solicitacao.nova("cliente-1", "CARTAO-GOLD", new BigDecimal("5000.00"), "cliente@exemplo.com",
                "token-123", AGORA);
    }

    public static DecisaoTomada decisao(String solicitacaoId, Decisao decisao) {
        return new DecisaoTomada("evento-1", solicitacaoId, decisao, "motivo", AGORA.plusSeconds(5));
    }
}
