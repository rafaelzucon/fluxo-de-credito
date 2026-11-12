package br.com.fluxocredito.motor;

import java.math.BigDecimal;

import org.springframework.stereotype.Component;

/**
 * Simula a consulta aos fatores de elegibilidade. Convenções para testes manuais:
 * clienteId que começa com "restrito" tem restrição cadastral; os demais têm score e limite derivados do hash.
 */
@Component
public class ConsultaFatoresElegibilidade {

    public FatoresElegibilidade consultar(String clienteId) {
        boolean restricao = clienteId.toLowerCase().startsWith("restrito");
        int hash = Math.abs(clienteId.hashCode());
        int score = 300 + hash % 701;
        BigDecimal limite = BigDecimal.valueOf(1_000L + (hash % 50) * 1_000L);
        return new FatoresElegibilidade(score, limite, restricao);
    }
}
