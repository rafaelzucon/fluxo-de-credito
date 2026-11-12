package br.com.fluxocredito.solicitacao.api;

import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import br.com.fluxocredito.solicitacao.dominio.SolicitacaoNaoEncontradaException;

@RestControllerAdvice
public class TratadorDeErros {

    @ExceptionHandler(SolicitacaoNaoEncontradaException.class)
    ProblemDetail naoEncontrada(SolicitacaoNaoEncontradaException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, e.getMessage());
    }
}
