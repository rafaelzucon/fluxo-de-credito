package br.com.fluxocredito.motor;

import java.math.BigDecimal;

/** Fatores de elegibilidade do cliente. No simulador são derivados do clienteId de forma determinística. */
public record FatoresElegibilidade(int score, BigDecimal limiteDisponivel, boolean restricaoCadastral) {
}
