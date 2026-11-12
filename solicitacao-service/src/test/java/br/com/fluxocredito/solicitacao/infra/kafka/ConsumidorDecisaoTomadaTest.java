package br.com.fluxocredito.solicitacao.infra.kafka;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import br.com.fluxocredito.solicitacao.Fixtures;
import br.com.fluxocredito.solicitacao.aplicacao.ProcessadorDecisao;
import br.com.fluxocredito.solicitacao.dominio.Decisao;
import br.com.fluxocredito.solicitacao.eventos.DecisaoTomada;

class ConsumidorDecisaoTomadaTest {

    private final ProcessadorDecisao processador = mock(ProcessadorDecisao.class);
    private final JsonMapper json = JsonMapper.builder().build();
    private final ConsumidorDecisaoTomada consumidor = new ConsumidorDecisaoTomada(processador, json);

    @Test
    void desserializaEDelegaAoProcessador() {
        DecisaoTomada decisao = Fixtures.decisao("s-1", Decisao.APROVADA);

        consumidor.consumir(json.writeValueAsString(decisao));

        verify(processador).processar(decisao);
    }

    @Test
    void payloadInvalidoVaiDiretoParaODlt() {
        assertThatThrownBy(() -> consumidor.consumir("{nao-e-json"))
                .isInstanceOf(MensagemInvalidaException.class);
        verifyNoInteractions(processador);
    }
}
