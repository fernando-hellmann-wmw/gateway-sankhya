package br.com.wmw.gatewaysankhya.integration;

import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.stereotype.Service;

import br.com.wmw.framework.util.LockUtil;
import br.com.wmw.framework.util.WatchTime;
import br.com.wmw.framework.util.database.metadata.Column;
import br.com.wmw.framework.util.database.metadata.MetadataUtil;
import br.com.wmw.framework.util.database.metadata.Table;

@Service
public class EntidadeGenericaToGatewaySankhya extends BaseEnvioToGatewaySankhya {

	private static final Log log = LogFactory.getLog(EntidadeGenericaToGatewaySankhya.class);
	private static final String sqlServico = "SELECT DSAPELIDO FROM TBWMWCONFIGENTIDADEINTEG WHERE NMENTIDADE = '";
	
	@Value("${url.envio.generico:}")
	private String urlEnvioGenerico;
	
	public void execute() {
		try {
			List<String> entidadeList = jdbcTemplate.queryForList("SELECT NMENTIDADE FROM TBWMWCONFIGENTIDADEINTEG WHERE DSTIPOINTEGRACAO = 'Sankhya' AND (FLENTIDADEIMPORTACAO <> 'S' OR FLENTIDADEIMPORTACAO IS NULL) AND FLATIVO = 'S'", String.class);
			execute(entidadeList);
		} catch (Exception e) {
			log.error("Ocorreu um erro ao buscar as entidades para envio de dados ao gateway sankhya.", e);
		}
	}
	
	
	public void execute(List<String> entidadeList) {
		for (String nmEntidade : entidadeList) {
			try {
				LockUtil.getInstance().initExportConcurrentControl(this.getClass().getSimpleName(), 2000, 300000);
				log.info("Inicio do envio da entidade " + nmEntidade + " para o gateway sankhya.");
				Table table = MetadataUtil.extractTableMetadata(jdbcTemplate.getDataSource(), exportTablePrefix + nmEntidade);
				if (table == null) {
					log.error("A tabela " + exportTablePrefix + nmEntidade + " não foi encontrada!");
					continue;
				}
				List<Map<String, Object>> envioList = getListEnvioGateway(nmEntidade);
				log.debug("Encontrados " + envioList.size() + " registro(s) para envio.");
				String nmServico;
				try {
					nmServico = jdbcTemplate.queryForObject(sqlServico + nmEntidade + "' AND DSTIPOINTEGRACAO = 'Sankhya' AND (FLENTIDADEIMPORTACAO <> 'S' OR FLENTIDADEIMPORTACAO IS NULL) AND FLATIVO = 'S'", String.class);
					if (nmServico == null) {
						log.error("Não foi encontrada a ConfigEntidadeInteg para a entidade " + nmEntidade);
						continue;
					}
				} catch (EmptyResultDataAccessException e) {
					log.error("Não foi encontrada a ConfigEntidadeInteg para a entidade " + nmEntidade);
					continue;
				}
				for (Map<String, Object> mapEntidade : envioList) {
					String jsonBody = "";
					jsonBody += addCabecalho(nmServico);
					jsonBody += addCampos(mapEntidade, table.getColumnMap());
					jsonBody += addRodape();
					WatchTime watch = new WatchTime(getClass().getName(), "envio" + nmEntidade);
					watch.start();
					String retorno = executeEnvioToGateway(jsonBody, nmEntidade, urlEnvioGenerico);
					log.info("Retorno do gateway em " + watch.stop() + " milessegundos");
					defineRetorno(mapEntidade, table.getPrimaryKeys(), retorno, nmEntidade);
				}
			} catch (Exception e) {
				log.error("Ocorreu um erro no envio de novos clientes para o gateway sankhya.", e);
				throw e;
			} finally {
				LockUtil.getInstance().endExportConcurrentControl(this.getClass().getSimpleName());
			}
		}
	}


	private String addCabecalho(String nmServico) {
		return """
				{
				"serviceName":"CRUDServiceProvider.saveRecord",
				 "requestBody":{
				    "dataSet":{
				       "rootEntity":" """ + nmServico + """ 
", 
				       "includePresentationFields":"S",
				       "dataRow":{
				          "localFields":{
				""";
	}
	
	private String addCampos(Map<String, Object> mapEntidade, Map<String, Column> columnMap) {
		return addCamposJson(mapEntidade, columnMap);
	}
	
	private String addRodape() {
		return "}}, \"entity\":{}}}}";
	}

	private int defineRetorno(Map<String, Object> mapEntidade, List<Column> pkList, String retorno, String nmEntidade) {
		if (retorno == null ) {
			log.error("Não foi possível definir o retorno da integração da entidade " + nmEntidade + ". Registro " + mapEntidade.toString());
			return 0;
		}
		String flControleErp = null;
		String dsMensagemErp = null;
		JSONObject json = new JSONObject(retorno);
		int status = json.getInt("status");
		if (status == STATUS_RETORNO_SUCESSO) {
			flControleErp = CONTROLE_ENVIADO_AUTOMATICO_OK;
			dsMensagemErp = "";
			log.info(nmEntidade + " integrado com sucesso");
		} else {
			flControleErp = CONTROLE_ENVIADO_AUTOMATICO_ERRO;
			dsMensagemErp = json.getString("statusMessage");
			dsMensagemErp = dsMensagemErp.replace("'", "\"");
			log.info(String.format("Não foi possível integrar o novocliente. Detalhes: %s", mapEntidade.toString()));
		}
		atualizaRetorno(mapEntidade, null, flControleErp, dsMensagemErp, pkList, nmEntidade, null);
		return status;
	}

}
