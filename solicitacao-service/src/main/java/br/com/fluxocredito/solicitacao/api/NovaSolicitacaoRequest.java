package br.com.fluxocredito.solicitacao.api;

import java.math.BigDecimal;

import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

public record NovaSolicitacaoRequest(
        @NotBlank @Size(max = 64) String clienteId,
        @NotBlank @Size(max = 64) String produtoCodigo,
        @NotNull @Positive @Digits(integer = 13, fraction = 2) BigDecimal valorSolicitado,
        @NotBlank @Email @Size(max = 254) String email,
        @NotBlank @Size(max = 512) String tokenDispositivo) {
}
