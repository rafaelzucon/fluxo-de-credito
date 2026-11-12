package br.com.fluxocredito.solicitacao.api;

import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import br.com.fluxocredito.solicitacao.Fixtures;
import br.com.fluxocredito.solicitacao.aplicacao.SolicitacaoService;
import br.com.fluxocredito.solicitacao.dominio.Solicitacao;
import br.com.fluxocredito.solicitacao.dominio.SolicitacaoNaoEncontradaException;

class SolicitacaoControllerTest {

    private final SolicitacaoService service = mock(SolicitacaoService.class);
    private final MockMvc mvc = MockMvcBuilders.standaloneSetup(new SolicitacaoController(service))
            .setControllerAdvice(new TratadorDeErros())
            .build();

    @Test
    void retorna202ComAMensagemPadrao() throws Exception {
        Solicitacao s = Fixtures.solicitacao();
        when(service.criar(any())).thenReturn(s);

        mvc.perform(post("/api/v1/solicitacoes").contentType(MediaType.APPLICATION_JSON).content("""
                        {"clienteId":"cliente-1","produtoCodigo":"CARTAO-GOLD","valorSolicitado":5000.00,
                         "email":"cliente@exemplo.com","tokenDispositivo":"token-123"}
                        """))
                .andExpect(status().isAccepted())
                .andExpect(header().string("Location", "/api/v1/solicitacoes/" + s.getId()))
                .andExpect(jsonPath("$.solicitacaoId", is(s.getId())))
                .andExpect(jsonPath("$.mensagem", is(SolicitacaoAceitaResponse.MENSAGEM_PADRAO)));
    }

    @Test
    void requisicaoInvalidaRetorna400() throws Exception {
        mvc.perform(post("/api/v1/solicitacoes").contentType(MediaType.APPLICATION_JSON).content("""
                        {"clienteId":"","produtoCodigo":"CARTAO-GOLD","valorSolicitado":-1,
                         "email":"invalido","tokenDispositivo":"token-123"}
                        """))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(service);
    }

    @Test
    void consultaRetornaOStatus() throws Exception {
        Solicitacao s = Fixtures.solicitacao();
        when(service.consultar(s.getId())).thenReturn(s);

        mvc.perform(get("/api/v1/solicitacoes/{id}", s.getId()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status", is("PENDENTE")));
    }

    @Test
    void consultaInexistenteRetorna404() throws Exception {
        when(service.consultar("x")).thenThrow(new SolicitacaoNaoEncontradaException("x"));

        mvc.perform(get("/api/v1/solicitacoes/x")).andExpect(status().isNotFound());
    }
}
