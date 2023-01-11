package br.com.wmw.gatewaysankhya.integration;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import br.com.wmw.framework.util.LockUtil;
import br.com.wmw.framework.util.ValueUtil;
import br.com.wmw.framework.util.database.metadata.Column;
import br.com.wmw.framework.util.database.metadata.MetadataUtil;
import br.com.wmw.framework.util.database.metadata.Table;

@Service
public class PedidoToGatewaySankhya {

	private final static String contentType = "Content-Type";
	private final static String authorization = "Authorization";
	private final static String bearer = "Bearer ";
	private final static String applicationJson = "application/json";
	public static final String CONTROLE_ENVIADO_AUTOMATICO_OK = "E";
	public static final String CONTROLE_ENVIADO_AUTOMATICO_ERRO = "X";
	public static final String NM_COLUNA_CONTROLE_ERP = "FLCONTROLEERP";
	public static final String NM_COLUNA_DSCONTROLE_ERP = "DSMENSAGEMCONTROLEERP";
	public static final String NM_COLUNA_NUPEDIDORELACIONADO = "NUPEDIDORELACIONADO";
	public static final String NMENTIDADEITEMPEDIDO = "ITEMPEDIDO";
	public static final String NMENTIDADEPEDIDO = "PEDIDO";
	private static final Log log = LogFactory.getLog(PedidoToGatewaySankhya.class);
	private static RestTemplate restTemplate = new RestTemplate();
	
	@Inject
	JdbcTemplate jdbcTemplate = new JdbcTemplate();
	@Inject
	private LoginGatewaySankhya loginGatewaySankhya;
	@Value("${url.envio.pedido:}")
	private String urlEnvioPedido;
	@Value("${token:}")
	private String token;
	@Value("${appkey:}")
	private String appkey;
	@Value("${export.table.prefix:}")
	private String exportTablePrefix;
	
	
	public void execute() {
		LockUtil.getInstance().initExportConcurrentControl(this.getClass().getSimpleName(), 2000, 300000);
		log.info("Inicio do envio de pedidos para o gateway sankhya.");
		try {
			Table pedidoTable = MetadataUtil.extractTableMetadata(jdbcTemplate.getDataSource(), exportTablePrefix + NMENTIDADEPEDIDO);
			List<Map<String, Object>> pedidoList = getListPedidoEnvioGateway();
			log.debug("Encontrados " + pedidoList.size() + " para envio.");
			for (Map<String, Object> mapPedido : pedidoList) {
				List<Map<String, Object>> itemPedidoList = getListItemPedidoEnvioGateway(pedidoTable.getPrimaryKeys());
				if (ValueUtil.isEmpty(itemPedidoList)) {
					log.error("Não foram encontrados itens para o pedido " + mapPedido.toString());
					continue;
				}
				String jsonBody = new String();
				jsonBody += addCabecalhoPedido();
				jsonBody += addCamposPedido(mapPedido);
				jsonBody += addCabecalhoItens();
				jsonBody += addCamposItensPedido(itemPedidoList);
				jsonBody += addRodape();
				String retorno = executeEnvioPedido(jsonBody);
				defineRetornoPedido(mapPedido, pedidoTable, retorno);
			}
		} catch (Exception e) {
			log.error("Ocorreu um erro no envio de pedidos para o gateway sankhya.", e);
		} finally {
			LockUtil.getInstance().endExportConcurrentControl(this.getClass().getSimpleName());
		}
	}

	

	private void defineRetornoPedido(Map<String, Object> mapPedido, Table pedidoTable, String retorno) {
		if (retorno == null ) {
			log.error("Não foi possível definir o retorno da integração do pedido " + mapPedido.toString());
			return;
		}
		String flControleErp = null;
		String dsMensagemErp = null;
		String nuPedidoRelacionado = null;
		JSONObject json = new JSONObject(retorno);
		int status = json.getInt("status");
		if (status == 1) {
			flControleErp = CONTROLE_ENVIADO_AUTOMATICO_OK;
			nuPedidoRelacionado = json.getJSONObject("responseBody").getJSONObject("pk").getJSONObject("NUNOTA").getString("$");
			log.info("Pedido integrado com sucesso");
		} else {
			flControleErp = CONTROLE_ENVIADO_AUTOMATICO_ERRO;
			dsMensagemErp = json.getString("statusMessage");
			log.info(String.format("Não foi possível integrar o pedido via WebService. Detalhes: %s", mapPedido.toString()));
		}
		String updateSql = getUpdateSql(mapPedido, nuPedidoRelacionado, flControleErp, dsMensagemErp);
		jdbcTemplate.update(updateSql);
	}



	private String getUpdateSql(Map<String, Object> mapPedido, String nuPedidoRelacionado, String flControleErp, String dsMensagemErp) {
		StringBuilder sql = new StringBuilder();
		sql.append("UPDATE ").append(exportTablePrefix).append(NMENTIDADEPEDIDO);
		sql.append(" SET ").append(NM_COLUNA_NUPEDIDORELACIONADO).append(" = ").append(nuPedidoRelacionado);
		sql.append(NM_COLUNA_CONTROLE_ERP).append(" = ").append(flControleErp);
		sql.append(NM_COLUNA_DSCONTROLE_ERP).append(" = ").append(dsMensagemErp);
		sql.append(" WHERE ");
		return null;
	}


	private String addCabecalhoPedido() {
		return """
				{
				   "serviceName":"CACSP.incluirNota",
				   "requestBody":{
				      "nota":{
				         "cabecalho":{""";
	}


	private String addCamposPedido(Map<String, Object> mapPedido) {
		String jsonPedido = addCamposJson(mapPedido);
		jsonPedido += "},";
		return jsonPedido;
	}


	private List<Map<String, Object>> getListPedidoEnvioGateway() {
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT * ");
		sql.append(" FROM ");
		sql.append(exportTablePrefix + NMENTIDADEPEDIDO);
		sql.append(" WHERE ");
		sql.append(NM_COLUNA_CONTROLE_ERP).append(" <> '");
		sql.append(CONTROLE_ENVIADO_AUTOMATICO_OK);
		sql.append("' OR " + NM_COLUNA_CONTROLE_ERP +" IS NULL");
		return jdbcTemplate.queryForList(sql.toString());
	}
	
	
	private List<Map<String, Object>> getListItemPedidoEnvioGateway(List<Column> primaryKeys) {
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT * ");
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
	

	private String addCamposJson(Map<String, Object> map) {
		StringBuilder campos = new StringBuilder();
		for (Map.Entry<String,Object> entry : map.entrySet()) {
			String nmCampo = entry.getKey();
			Object valor = entry.getValue();
			if (NM_COLUNA_CONTROLE_ERP.equalsIgnoreCase(nmCampo)) {
				continue;
			}
			campos.append("\"").append(nmCampo).append("\":{");
			campos.append("\"$\":").append(valor).append("\"");
			campos.append("},");
		}
		campos.deleteCharAt(campos.length() - 1);
		return campos.toString();
	}

	private String addCabecalhoItens() {
		return """
				     "itens":{
				        "INFORMARPRECO":"True",
				        "item":[
				""";
	}
	
	private String addCamposItensPedido(List<Map<String, Object>> itemPedidoList) {
		StringBuilder campos = new StringBuilder();
		for (Map<String, Object> map : itemPedidoList) {
			campos.append("{");
			addCamposJson(map);
			campos.append("},");
		}
		campos.deleteCharAt(campos.length() - 1);
		campos.append("]");
		return campos.toString();
	}
	
	private String addRodape() {
		return "}}}}";
	}

	private String executeEnvioPedido(String body) {
		String loginToken = loginGatewaySankhya.login();
		log.info("Serviço de envio de pedido sendo executado no gateway sankhya.");
		log.debug("Url: " + urlEnvioPedido + " - Token: " + token + " - AppKey: " + appkey);
		HttpHeaders headerPost = new HttpHeaders();
		headerPost.set(contentType, applicationJson);
		headerPost.set( "token", getParamDecode(token));
		headerPost.set( "appkey", getParamDecode(appkey));
		headerPost.set(authorization, bearer + loginToken);
		HttpEntity<String> postRequest = new HttpEntity<>(body, headerPost);
		try {
			ResponseEntity<String> result = restTemplate.postForEntity(urlEnvioPedido, postRequest, String.class);
			return result.getBody();
		 } catch (Exception e) {
			 log.error("Ocorreu um erro no envio do pedido ao gateway sankhya.", e);
			 return "ERRO " + e.getMessage();
	     }
	}
	
	private String getParamDecode(String param) {
		return new String(Base64.decodeBase64(param));
	}
}
