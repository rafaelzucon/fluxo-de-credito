package br.com.fluxocredito.motor;

import java.time.Clock;
import java.util.UUID;

import org.springframework.stereotype.Component;

/** Regras de decisão do simulador do motor de crédito. */
@Component
public class MotorDecisao {

    static final int SCORE_MINIMO = 500;

    private final ConsultaFatoresElegibilidade fatores;
    private final Clock clock;

    public MotorDecisao(ConsultaFatoresElegibilidade fatores, Clock clock) {
        this.fatores = fatores;
        this.clock = clock;
    }

    public DecisaoTomada decidir(SolicitacaoCriada solicitacao) {
        FatoresElegibilidade f = fatores.consultar(solicitacao.clienteId());
        String motivo;
        if (f.restricaoCadastral()) {
            motivo = "Restrição cadastral";
        } else if (f.score() < SCORE_MINIMO) {
            motivo = "Score insuficiente";
        } else if (solicitacao.valorSolicitado().compareTo(f.limiteDisponivel()) > 0) {
            motivo = "Valor acima do limite disponível";
        } else {
            return new DecisaoTomada(UUID.randomUUID().toString(), solicitacao.solicitacaoId(), "APROVADA",
                    "Cliente elegível", clock.instant());
        }
        return new DecisaoTomada(UUID.randomUUID().toString(), solicitacao.solicitacaoId(), "NEGADA", motivo,
                clock.instant());
    }
}
