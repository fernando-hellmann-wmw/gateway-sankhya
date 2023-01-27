package br.com.wmw.gatewaysankhya.integration;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import br.com.wmw.framework.util.LockUtil;
import br.com.wmw.framework.util.ValueUtil;
import br.com.wmw.framework.util.WatchTime;
import br.com.wmw.framework.util.database.metadata.MetadataUtil;
import br.com.wmw.framework.util.database.metadata.Table;
import br.com.wmw.gatewaysankhya.service.PersistenceService;

@Service
public class SankhyaIntegrator {

	private static final String LOCK_DOMINIO = "SANKHYA2TMPINT";
	private static Log log = LogFactory.getLog(SankhyaIntegrator.class);
	private static final String sqlServico = "SELECT DSAPELIDO FROM TBWMWCONFIGENTIDADEINTEG WHERE NMENTIDADE = '";
	
	@Inject
	private PersistenceService persistenceService;
	@Inject
	private ImportaDadosGatewaySankhya importaDadosGatewaySankhya;
	@Inject
	JdbcTemplate jdbcTemplate = new JdbcTemplate();
	@Value("${import.table.prefix:}")
	private String importTablePrefix;
	
	public void execute() {
		List<String> entidadeList = jdbcTemplate.queryForList("SELECT NMENTIDADE FROM TBWMWCONFIGENTIDADEINTEG WHERE DSTIPOINTEGRACAO = 'SANKHYA' AND FLENTIDADEIMPORTACAO = 'S'", String.class);
		if (ValueUtil.isEmpty(entidadeList)) {
			log.warn("Nenhuma entidade encontrada para a importação de dados.");
			return;
		}
		execute(entidadeList);
	}
	
	public void execute(List<String> entidades) {
		log.info("Iniciando importação de dados do Gateway Sankhya");
		try {
			LockUtil.getInstance().initExportConcurrentControl(LOCK_DOMINIO, 5000, 60000);
			for (String nmEntidade : entidades) {
				String nmServico;
				try {
					nmServico = jdbcTemplate.queryForObject(sqlServico + nmEntidade + "' AND DSTIPOINTEGRACAO = 'SANKHYA' AND FLENTIDADEIMPORTACAO = 'S'", String.class);
					if (nmServico == null) {
						log.error("Não foi encontrada a ConfigEntidadeInteg para a entidade " + nmEntidade);
						continue;
					}
				} catch (EmptyResultDataAccessException e) {
					log.error("Não foi encontrada a ConfigEntidadeInteg para a entidade " + nmEntidade);
					continue;
				}
				log.info("Iniciando importação de dados para a entidade " + nmServico);
				Table table = MetadataUtil.extractTableMetadata(jdbcTemplate.getDataSource(), importTablePrefix + nmEntidade);
				if (table == null) {
					log.error("A tabela " + importTablePrefix + nmEntidade + " não foi encontrada!");
					continue;
				}
				String dsFiltrosConsulta = getFiltrosConsulta(table.getTableName(), nmEntidade);
				WatchTime watch = new WatchTime(getClass().getName(), "importaDados" + nmEntidade);
				watch.start();
				String jSonResult = importaDadosGatewaySankhya.importaDados(nmServico, table.getColumns(), dsFiltrosConsulta);
				log.debug("Retorno do gateway em " + watch.stop() + " milessegundos");
				String retorno = persistenceService.persisteDados(nmEntidade, jSonResult);
				if (retorno != null) log.info(retorno);
			}
		} catch (Exception e) {
			log.error(e);
		} finally {
			LockUtil.getInstance().endExportConcurrentControl(LOCK_DOMINIO);
			log.info("Finalizada importação de dados do Gateway Sankhya");
		}
	}

	private String getFiltrosConsulta(String tableName, String nmEntidade) {
		String jsonFilter = null;
		String sql = "SELECT MAX(DTALTER) FROM " + tableName;
		String dtMax;
		try {
			dtMax = jdbcTemplate.queryForObject(sql, String.class);
			if (ValueUtil.isNotEmpty(dtMax)) {
				jsonFilter = "\"$\": \"TO_CHAR(this.DTALTER,'YYYY-MM-DD HH:MM:SS') > '" + dtMax + "'\"";
			}
		} catch (BadSqlGrammarException e) {
			log.debug("Não foi encontrada a coluna de controle de data para a tabela " + tableName);
		} catch (EmptyResultDataAccessException e) {
			log.debug("Não foi encontrada a data máxima da última importação de dados para a tabela " + tableName);
		}
		try {
			sql = "SELECT DSPARAMETROSADICIONAIS FROM TBWMWCONFIGENTIDADEINTEG WHERE NMENTIDADE = '" + nmEntidade + "'";
			String dsFiltrosAdicionais = jdbcTemplate.queryForObject(sql, String.class);
			if (ValueUtil.isEmpty(dsFiltrosAdicionais)) {
				return jsonFilter;
			}
			if (jsonFilter == null) {
				jsonFilter = "\"$\": \"";
			} else {
				jsonFilter = jsonFilter.substring(0, jsonFilter.length() - 1);
				jsonFilter += " AND ";
			}
			jsonFilter += dsFiltrosAdicionais + "\"";
		} catch (EmptyResultDataAccessException e) {
			log.debug("Não foram encontrados filtros adicionais para a importação de dados da tabela " + tableName);
		}
		return jsonFilter;
	}

}
