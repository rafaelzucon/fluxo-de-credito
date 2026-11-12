package br.com.fluxocredito.solicitacao.api;

import java.net.URI;

import jakarta.validation.Valid;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import br.com.fluxocredito.solicitacao.aplicacao.SolicitacaoService;
import br.com.fluxocredito.solicitacao.dominio.Solicitacao;

@RestController
@RequestMapping("/api/v1/solicitacoes")
public class SolicitacaoController {

    private final SolicitacaoService service;

    public SolicitacaoController(SolicitacaoService service) {
        this.service = service;
    }

    @PostMapping
    public ResponseEntity<SolicitacaoAceitaResponse> criar(@Valid @RequestBody NovaSolicitacaoRequest request) {
        Solicitacao solicitacao = service.criar(request);
        return ResponseEntity.accepted()
                .location(URI.create("/api/v1/solicitacoes/" + solicitacao.getId()))
                .body(SolicitacaoAceitaResponse.para(solicitacao.getId()));
    }

    @GetMapping("/{id}")
    public SolicitacaoResponse consultar(@PathVariable String id) {
        return SolicitacaoResponse.de(service.consultar(id));
    }
}
