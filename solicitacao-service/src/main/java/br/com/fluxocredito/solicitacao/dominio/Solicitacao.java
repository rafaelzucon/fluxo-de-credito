package br.com.fluxocredito.solicitacao.dominio;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

@Entity
@Table(name = "solicitacao")
public class Solicitacao {

    @Id
    @Column(length = 36)
    private String id;

    @Column(name = "cliente_id", nullable = false)
    private String clienteId;

    @Column(name = "produto_codigo", nullable = false)
    private String produtoCodigo;

    @Column(name = "valor_solicitado", nullable = false, precision = 15, scale = 2)
    private BigDecimal valorSolicitado;

    @Column(nullable = false)
    private String email;

    @Column(name = "token_dispositivo", nullable = false)
    private String tokenDispositivo;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private StatusSolicitacao status;

    @Column(name = "motivo_decisao")
    private String motivoDecisao;

    @Column(name = "criado_em", nullable = false)
    private Instant criadoEm;

    @Column(name = "atualizado_em", nullable = false)
    private Instant atualizadoEm;

    @Version
    private Long versao;

    protected Solicitacao() {
    }

    public static Solicitacao nova(String clienteId, String produtoCodigo, BigDecimal valorSolicitado,
                                   String email, String tokenDispositivo, Instant agora) {
        var s = new Solicitacao();
        s.id = UUID.randomUUID().toString();
        s.clienteId = Objects.requireNonNull(clienteId);
        s.produtoCodigo = Objects.requireNonNull(produtoCodigo);
        s.valorSolicitado = Objects.requireNonNull(valorSolicitado);
        s.email = Objects.requireNonNull(email);
        s.tokenDispositivo = Objects.requireNonNull(tokenDispositivo);
        s.status = StatusSolicitacao.PENDENTE;
        s.criadoEm = agora;
        s.atualizadoEm = agora;
        return s;
    }

    public void marcarPendentePublicacao(Instant agora) {
        if (status == StatusSolicitacao.PENDENTE) {
            status = StatusSolicitacao.PENDENTE_PUBLICACAO;
            atualizadoEm = agora;
        }
    }

    public void marcarPublicada(Instant agora) {
        if (status == StatusSolicitacao.PENDENTE_PUBLICACAO) {
            status = StatusSolicitacao.PENDENTE;
            atualizadoEm = agora;
        }
    }

    public void registrarDecisao(Decisao decisao, String motivo, Instant agora) {
        status = decisao.paraStatus();
        motivoDecisao = motivo;
        atualizadoEm = agora;
    }

    public String getId() {
        return id;
    }

    public String getClienteId() {
        return clienteId;
    }

    public String getProdutoCodigo() {
        return produtoCodigo;
    }

    public BigDecimal getValorSolicitado() {
        return valorSolicitado;
    }

    public String getEmail() {
        return email;
    }

    public String getTokenDispositivo() {
        return tokenDispositivo;
    }

    public StatusSolicitacao getStatus() {
        return status;
    }

    public String getMotivoDecisao() {
        return motivoDecisao;
    }

    public Instant getCriadoEm() {
        return criadoEm;
    }

    public Instant getAtualizadoEm() {
        return atualizadoEm;
    }
}
