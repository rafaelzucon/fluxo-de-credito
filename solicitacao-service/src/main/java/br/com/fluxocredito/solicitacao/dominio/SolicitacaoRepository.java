package br.com.fluxocredito.solicitacao.dominio;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;

public interface SolicitacaoRepository extends JpaRepository<Solicitacao, String> {

    List<Solicitacao> findTop50ByStatusOrderByCriadoEmAsc(StatusSolicitacao status);
}
