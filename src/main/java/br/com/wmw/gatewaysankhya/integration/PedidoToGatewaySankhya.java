package br.com.wmw.gatewaysankhya.integration;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import br.com.wmw.framework.util.LockUtil;
import br.com.wmw.framework.util.ValueUtil;
import br.com.wmw.framework.util.WatchTime;
import br.com.wmw.framework.util.database.metadata.Column;
import br.com.wmw.framework.util.database.metadata.MetadataUtil;
import br.com.wmw.framework.util.database.metadata.Table;

@Service
public class PedidoToGatewaySankhya extends BaseEnvioToGatewaySankhya {

	public static final String NMENTIDADEITEMPEDIDO = "ITEMPEDIDO";
	public static final String NMENTIDADEPEDIDO = "PEDIDO";
	private static final Log log = LogFactory.getLog(PedidoToGatewaySankhya.class);
	
	
	@Value("${url.envio.pedido:}")
	private String urlEnvioPedido;
	
	
	public void execute() {
		LockUtil.getInstance().initExportConcurrentControl(this.getClass().getSimpleName(), 2000, 300000);
		log.info("Inicio do envio de pedidos para o gateway sankhya.");
		try {
			Table pedidoTable = MetadataUtil.extractTableMetadata(jdbcTemplate.getDataSource(), exportTablePrefix + NMENTIDADEPEDIDO);
			Table itemPedidoTable = MetadataUtil.extractTableMetadata(jdbcTemplate.getDataSource(), exportTablePrefix + NMENTIDADEITEMPEDIDO);
			if (! validaTabelasEnvioPedido(pedidoTable, itemPedidoTable)) {
				return;
			}
			List<Map<String, Object>> pedidoList = getListEnvioGateway(NMENTIDADEPEDIDO);
			log.debug("Encontrados " + pedidoList.size() + " pedido(s) para envio.");
			for (Map<String, Object> mapPedido : pedidoList) {
				List<Map<String, Object>> itemPedidoList = getListItemPedidoEnvioGateway(pedidoTable.getPrimaryKeys());
				if (ValueUtil.isEmpty(itemPedidoList)) {
					log.error("Não foram encontrados itens para o pedido " + mapPedido.toString());
					continue;
				}
				String jsonBody = "";
				jsonBody += addCabecalhoPedido();
				jsonBody += addCamposPedido(mapPedido, pedidoTable.getColumnMap());
				jsonBody += addCabecalhoItens();
				jsonBody += addCamposItensPedido(itemPedidoList, itemPedidoTable.getColumnMap());
				jsonBody += addRodape();
				WatchTime watch = new WatchTime(getClass().getName(), "envioPedido");
				watch.start();
				String retorno = executeEnvioToGateway(jsonBody, NMENTIDADEPEDIDO, urlEnvioPedido);
				log.debug("Retorno do gateway em " + watch.stop() + " milessegundos");
				int statusRetorno = defineRetornoPedido(mapPedido, pedidoTable.getPrimaryKeys(), retorno);
				if (statusRetorno == STATUS_RETORNO_SUCESSO && ValueUtil.isNotEmpty(urlProcedureDados2Erp)) {
					Object cdEmpresa = mapPedido.get(NM_COLUNA_CDEMPRESA);
					Object cdRepresentante = mapPedido.get(NM_COLUNA_CDREPRESENTANTE);
					acionaProcedureDados2Erp(cdEmpresa, cdRepresentante);
				}
			}
		} catch (Exception e) {
			log.error("Ocorreu um erro no envio de pedidos para o gateway sankhya.", e);
			throw e;
		} finally {
			LockUtil.getInstance().endExportConcurrentControl(this.getClass().getSimpleName());
		}
	}


	private boolean validaTabelasEnvioPedido(Table pedidoTable, Table itemPedidoTable) {
		if (pedidoTable == null) {
			log.error("A tabela " + exportTablePrefix + NMENTIDADEPEDIDO + " não foi encontrada!");
			return false;
		}
		if (itemPedidoTable == null) {
			log.error("A tabela " + exportTablePrefix + NMENTIDADEITEMPEDIDO + " não foi encontrada!");
			return false;
		}
		if (ValueUtil.isEmpty(pedidoTable.getPrimaryKeys())) {
			log.error("A tabela " + exportTablePrefix + NMENTIDADEPEDIDO + " não possui chave primária!");
			return false;
		}
		return true;
	}

	

	private int defineRetornoPedido(Map<String, Object> mapPedido, List<Column> pkList, String retorno) {
		if (retorno == null ) {
			log.error("Não foi possível definir o retorno da integração do pedido " + mapPedido.toString());
			return 0;
		}
		String flControleErp = null;
		String dsMensagemErp = null;
		String nuPedidoRelacionado = null;
		JSONObject json = new JSONObject(retorno);
		int status = json.getInt("status");
		if (status == STATUS_RETORNO_SUCESSO) {
			flControleErp = CONTROLE_ENVIADO_AUTOMATICO_OK;
			nuPedidoRelacionado = json.getJSONObject("responseBody").getJSONObject("pk").getJSONObject("NUNOTA").getString("$");
			dsMensagemErp = "";
			log.info("Pedido integrado com sucesso");
		} else {
			flControleErp = CONTROLE_ENVIADO_AUTOMATICO_ERRO;
			dsMensagemErp = json.getString("statusMessage");
			dsMensagemErp = dsMensagemErp.replace("'", "\"");
			log.info(String.format("Não foi possível integrar o pedido. Detalhes: %s", mapPedido.toString()));
		}
		PreparedStatement ps = null;
		try {
			ps = getPreparedStatement(mapPedido, nuPedidoRelacionado, flControleErp, dsMensagemErp, pkList, NMENTIDADEPEDIDO, NM_COLUNA_NUPEDIDORELACIONADO);
			ps.executeUpdate();
		} catch (Exception e) {
			log.error("Ocorreu um erro ao atualizar o retorno do pedido " + mapPedido.toString(), e);
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



	private String addCabecalhoPedido() {
		return """
				{
				   "serviceName":"CACSP.incluirNota",
				   "requestBody":{
				      "nota":{
				         "cabecalho":{""";
	}


	private String addCamposPedido(Map<String, Object> mapPedido, Map<String, Column> columnMap) {
		String jsonPedido = addCamposJson(mapPedido, columnMap);
		jsonPedido += "},";
		return jsonPedido;
	}


	
	private List<Map<String, Object>> getListItemPedidoEnvioGateway(List<Column> primaryKeys) {
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT TB.* ");
		sql.append(" FROM ");
		sql.append(exportTablePrefix + NMENTIDADEITEMPEDIDO + " TB");
		sql.append(" JOIN " + exportTablePrefix + NMENTIDADEPEDIDO + " PED ON");
		for (Column pkColumn : primaryKeys) {
			sql.append(" TB. ").append(pkColumn.getColumnName());	
			sql.append(" = PED. ").append(pkColumn.getColumnName());	
			sql.append(" AND ");	
		}
		sql.delete(sql.length() - 5, sql.length() - 1);
		return jdbcTemplate.queryForList(sql.toString());
	}
	

	private String addCabecalhoItens() {
		return """
				     "itens":{
				        "INFORMARPRECO":"True",
				        "item":[
				""";
	}
	
	private String addCamposItensPedido(List<Map<String, Object>> itemPedidoList, Map<String, Column> columnMap) {
		StringBuilder campos = new StringBuilder();
		for (Map<String, Object> map : itemPedidoList) {
			campos.append("{");
			campos.append(addCamposJson(map, columnMap));
			campos.append("},");
		}
		campos.deleteCharAt(campos.length() - 1);
		campos.append("]");
		return campos.toString();
	}
	
	private String addRodape() {
		return "}}}}";
	}
	
}
