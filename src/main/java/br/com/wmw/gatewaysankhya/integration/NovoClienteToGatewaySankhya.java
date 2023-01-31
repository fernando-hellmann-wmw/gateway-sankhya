package br.com.wmw.gatewaysankhya.integration;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import br.com.wmw.framework.util.LockUtil;
import br.com.wmw.framework.util.ValueUtil;
import br.com.wmw.framework.util.WatchTime;
import br.com.wmw.framework.util.database.metadata.Column;
import br.com.wmw.framework.util.database.metadata.MetadataUtil;
import br.com.wmw.framework.util.database.metadata.Table;

@Service
public class NovoClienteToGatewaySankhya extends BaseEnvioToGatewaySankhya {

	private static final Log log = LogFactory.getLog(NovoClienteToGatewaySankhya.class);
	public static final String NMENTIDADENOVOCLIENTE = "NOVOCLIENTE";
	public static final String NM_COLUNA_CDNOVOCLIENTE = "CDNOVOCLIENTE";
	
	@Value("${url.envio.novocliente:}")
	private String urlEnvioNovoCliente;
	
	
	public void execute() {
		LockUtil.getInstance().initExportConcurrentControl(this.getClass().getSimpleName(), 2000, 300000);
		log.info("Inicio do envio de novos clientes para o gateway sankhya.");
		log.trace("Trace");
		try {
			Table novoClienteTable = MetadataUtil.extractTableMetadata(jdbcTemplate.getDataSource(), exportTablePrefix + NMENTIDADENOVOCLIENTE);
			if (! validateTable(novoClienteTable)) {
				return;
			}
			List<Map<String, Object>> novoClienteList = getListEnvioGateway(NMENTIDADENOVOCLIENTE);
			log.debug("Encontrados " + novoClienteList.size() + " novo(s) cliente(s) para envio.");
			for (Map<String, Object> mapNovoCliente : novoClienteList) {
				String jsonBody = "";
				jsonBody += addCabecalhoNovoCliente();
				jsonBody += addCamposNovoCliente(mapNovoCliente, novoClienteTable.getColumnMap());
				jsonBody += addRodape();
				WatchTime watch = new WatchTime(getClass().getName(), "envioNovoCliente");
				watch.start();
				String retorno = executeEnvioToGateway(jsonBody, NMENTIDADENOVOCLIENTE, urlEnvioNovoCliente);
				log.info("Retorno do gateway em " + watch.stop() + " milessegundos");
				int statusRetorno = defineRetornoPedido(mapNovoCliente, novoClienteTable.getPrimaryKeys(), retorno);
				if (statusRetorno == STATUS_RETORNO_SUCESSO && ValueUtil.isNotEmpty(urlProcedureDados2Erp)) {
					Object cdEmpresa = mapNovoCliente.get(NM_COLUNA_CDEMPRESA);
					Object cdRepresentante = mapNovoCliente.get(NM_COLUNA_CDREPRESENTANTE);
					acionaProcedureDados2Erp(cdEmpresa, cdRepresentante);
				}
			}
		} catch (Exception e) {
			log.error("Ocorreu um erro no envio de novos clientes para o gateway sankhya.", e);
			throw e;
		} finally {
			LockUtil.getInstance().endExportConcurrentControl(this.getClass().getSimpleName());
		}
	}


	private boolean validateTable(Table novoClienteTable) {
		if (novoClienteTable == null) {
			log.error("A tabela " + exportTablePrefix + NMENTIDADENOVOCLIENTE + " não foi encontrada!");
			return false; 
		}
		if (ValueUtil.isEmpty(novoClienteTable.getPrimaryKeys())) {
			log.error("A tabela " + exportTablePrefix + NMENTIDADENOVOCLIENTE + " não possui chave primária!");
			return false;
		}
		return true;
	}


	private String addCabecalhoNovoCliente() {
		return """
				{
				   "serviceName":"CRUDServiceProvider.saveRecord",
				   "requestBody":{
				      "dataSet":{
				         "rootEntity":"Parceiro",
				         "includePresentationFields":"S",
				         "dataRow":{
				            "localFields":{""";
	}
	
	private String addCamposNovoCliente(Map<String, Object> mapNovoCliente, Map<String, Column> columnMap) {
		String jsonNovoCliente = addCamposJson(mapNovoCliente, columnMap);
		jsonNovoCliente += "}},";
		jsonNovoCliente += """
				"entity":{
				         "fieldset":{
				            "list":"CODPARC"
				         }
				      }""";
		return jsonNovoCliente;
	}

	private String addRodape() {
		return "}}}";
	}
	
	private int defineRetornoPedido(Map<String, Object> mapNovoCliente, List<Column> pkList, String retorno) {
		if (retorno == null ) {
			log.error("Não foi possível definir o retorno da integração do novocliente " + mapNovoCliente.toString());
			return 0;
		}
		String flControleErp = null;
		String dsMensagemErp = null;
		String cdNovoCliente = null;
		JSONObject json = new JSONObject(retorno);
		int status = json.getInt("status");
		if (status == STATUS_RETORNO_SUCESSO) {
			flControleErp = CONTROLE_ENVIADO_AUTOMATICO_OK;
			cdNovoCliente = json.getJSONObject("responseBody").getJSONObject("entities").getJSONObject("entity").getJSONObject("CODPARC").getString("$");
			dsMensagemErp = "";
			log.info("NovoCliente integrado com sucesso");
		} else {
			flControleErp = CONTROLE_ENVIADO_AUTOMATICO_ERRO;
			dsMensagemErp = json.getString("statusMessage");
			dsMensagemErp = dsMensagemErp.replace("'", "\"");
			log.info(String.format("Não foi possível integrar o novocliente. Detalhes: %s", mapNovoCliente.toString()));
		}
		PreparedStatement ps = null;
		try {
			ps = getPreparedStatement(mapNovoCliente, cdNovoCliente, flControleErp, dsMensagemErp, pkList, NMENTIDADENOVOCLIENTE, NM_COLUNA_CDNOVOCLIENTE);
			ps.executeUpdate();
		} catch (Exception e) {
			log.error("Ocorreu um erro ao atualizar o retorno do novocliente " + mapNovoCliente.toString(), e);
		} finally {
			try {
				if (ps != null) {
					ps.close();
				}
			} catch (SQLException e) {
				log.error(e);
			}
		}
		return status;
	}

	
}
