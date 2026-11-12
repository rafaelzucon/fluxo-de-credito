package br.com.fluxocredito.solicitacao.dominio;

import static br.com.fluxocredito.solicitacao.Fixtures.AGORA;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import br.com.fluxocredito.solicitacao.Fixtures;

class SolicitacaoTest {

    @Test
    void novaSolicitacaoComecaPendente() {
        Solicitacao s = Fixtures.solicitacao();

        assertThat(s.getId()).isNotBlank();
        assertThat(s.getStatus()).isEqualTo(StatusSolicitacao.PENDENTE);
        assertThat(s.getCriadoEm()).isEqualTo(AGORA);
    }

    @Test
    void marcaPendentePublicacaoEDepoisPublicada() {
        Solicitacao s = Fixtures.solicitacao();

        s.marcarPendentePublicacao(AGORA.plusSeconds(1));
        assertThat(s.getStatus()).isEqualTo(StatusSolicitacao.PENDENTE_PUBLICACAO);

        s.marcarPublicada(AGORA.plusSeconds(2));
        assertThat(s.getStatus()).isEqualTo(StatusSolicitacao.PENDENTE);
        assertThat(s.getAtualizadoEm()).isEqualTo(AGORA.plusSeconds(2));
    }

    @Test
    void naoVoltaParaPendenteDepoisDeDecidida() {
        Solicitacao s = Fixtures.solicitacao();
        s.marcarPendentePublicacao(AGORA);
        // a decisão pode chegar antes do job de republicação marcar a solicitação como publicada
        s.registrarDecisao(Decisao.APROVADA, "ok", AGORA);

        s.marcarPublicada(AGORA.plusSeconds(1));
        s.marcarPendentePublicacao(AGORA.plusSeconds(1));

        assertThat(s.getStatus()).isEqualTo(StatusSolicitacao.APROVADA);
    }

    @Test
    void registraDecisaoNegada() {
        Solicitacao s = Fixtures.solicitacao();

        s.registrarDecisao(Decisao.NEGADA, "Score insuficiente", AGORA.plusSeconds(3));

        assertThat(s.getStatus()).isEqualTo(StatusSolicitacao.NEGADA);
        assertThat(s.getMotivoDecisao()).isEqualTo("Score insuficiente");
    }
}
