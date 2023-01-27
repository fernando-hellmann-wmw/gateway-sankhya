package br.com.wmw.gatewaysankhya.integration;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.StatementCreatorUtils;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import br.com.wmw.framework.util.DateUtil;
import br.com.wmw.framework.util.WebServiceUtil;
import br.com.wmw.framework.util.database.metadata.Column;
import kong.unirest.HttpResponse;

@Service
public class BaseEnvioToGatewaySankhya {

	private static final Log log = LogFactory.getLog(BaseEnvioToGatewaySankhya.class);
	
	protected final static String contentType = "Content-Type";
	protected final static String authorization = "Authorization";
	protected final static String bearer = "Bearer ";
	protected final static String applicationJson = "application/json";
	protected static final String CONTROLE_ENVIADO_AUTOMATICO_OK = "E";
	protected static final String CONTROLE_ENVIADO_AUTOMATICO_ERRO = "X";
	protected static final String NM_COLUNA_CONTROLE_ERP = "FLCONTROLEERP";
	protected static final String NM_COLUNA_DSCONTROLE_ERP = "DSMENSAGEMCONTROLEERP";
	protected static final String NM_COLUNA_CDEMPRESA = "CDEMPRESA";
	protected static final String NM_COLUNA_CDREPRESENTANTE = "CDREPRESENTANTE";
	protected static final String NM_COLUNA_CDNOVOCLIENTE = "CDNOVOCLIENTE";
	protected static final String NM_COLUNA_NUPEDIDORELACIONADO = "NUPEDIDORELACIONADO";
	protected static final int STATUS_RETORNO_SUCESSO = 1;

	protected static RestTemplate restTemplate = new RestTemplate();
	
	@Inject
	protected JdbcTemplate jdbcTemplate = new JdbcTemplate();
	@Inject
	protected LoginGatewaySankhya loginGatewaySankhya;
	@Value("${token:}")
	protected String token;
	@Value("${appkey:}")
	protected String appkey;
	@Value("${export.table.prefix:}")
	protected String exportTablePrefix;
	@Value("${url.procedure.dados2erp:}")
	protected String urlProcedureDados2Erp;
	
	
	protected List<Map<String, Object>> getListEnvioGateway(String nmEntidade) {
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT * ");
		sql.append(" FROM ");
		sql.append(exportTablePrefix + nmEntidade);
		sql.append(" WHERE ");
		sql.append(NM_COLUNA_CONTROLE_ERP).append(" <> '");
		sql.append(CONTROLE_ENVIADO_AUTOMATICO_OK);
		sql.append("' OR " + NM_COLUNA_CONTROLE_ERP +" IS NULL");
		return jdbcTemplate.queryForList(sql.toString());
	}
	
	protected String addCamposJson(Map<String, Object> map, Map<String, Column> columnMap) {
		StringBuilder campos = new StringBuilder();
		for (Map.Entry<String,Object> entry : map.entrySet()) {
			String nmCampo = entry.getKey();
			Object valor = entry.getValue();
			Column column = columnMap.get(nmCampo.toUpperCase());
			if (ignoraCampo(nmCampo)) {
				continue;
			}
			campos.append("\"").append(nmCampo).append("\":{");
			if (valor != null) {
				campos.append("\"$\":\"").append(getValorFormatado(valor, column.getDataType())).append("\"");
			}
			campos.append("},");
		}
		campos.deleteCharAt(campos.length() - 1);
		return campos.toString();
	}



	protected Object getValorFormatado(Object valor, int dataType) {
		if (dataType == Types.DATE) {
			return DateUtil.getDataAsString((Date) valor);
		} else if (dataType == Types.TIMESTAMP) {
			return DateUtil.dateTimeToString((Date) valor);
		}
		return valor;
	}



	protected boolean ignoraCampo(String nmCampo) {
		return NM_COLUNA_CONTROLE_ERP.equalsIgnoreCase(nmCampo)
				|| NM_COLUNA_DSCONTROLE_ERP.equalsIgnoreCase(nmCampo)
				|| NM_COLUNA_CDEMPRESA.equalsIgnoreCase(nmCampo)
				|| NM_COLUNA_CDREPRESENTANTE.equalsIgnoreCase(nmCampo)
				|| NM_COLUNA_CDNOVOCLIENTE.equalsIgnoreCase(nmCampo)
				|| NM_COLUNA_NUPEDIDORELACIONADO.equalsIgnoreCase(nmCampo);
	}
	
	protected String executeEnvioToGateway(String body, String nmEntidade, String urlEnvio) {
		String loginToken = loginGatewaySankhya.login();
		log.info("Serviço de envio de " + nmEntidade + " sendo executado no gateway sankhya.");
		log.debug("Url: " + urlEnvio + " - Token: " + token + " - AppKey: " + appkey);
		HttpHeaders headerPost = new HttpHeaders();
		headerPost.set(contentType, applicationJson);
		headerPost.set("token", getParamDecode(token));
		headerPost.set("appkey", getParamDecode(appkey));
		headerPost.set(authorization, bearer + loginToken);
		HttpEntity<String> postRequest = new HttpEntity<>(body, headerPost);
		try {
			ResponseEntity<String> result = restTemplate.postForEntity(urlEnvio, postRequest, String.class);
			return result.getBody();
		 } catch (Exception e) {
			 log.error("Ocorreu um erro no envio de " + nmEntidade + " ao gateway sankhya.", e);
			 return "ERRO " + e.getMessage();
	     }
	}
	
	protected String getParamDecode(String param) {
		return new String(Base64.decodeBase64(param));
	}
	
	protected PreparedStatement getPreparedStatement(Map<String, Object> map, String cdRetorno, String flControleErp, String dsMensagemErp, List<Column> pkList, String nmEntidade, String nmColunaRetorno) throws SQLException {
		PreparedStatement ps = null;
		StringBuilder sql = new StringBuilder();
		sql.append("UPDATE ").append(exportTablePrefix).append(nmEntidade);
		sql.append(" SET ");
		sql.append(NM_COLUNA_CONTROLE_ERP).append(" = '").append(flControleErp).append("'");
		if (cdRetorno != null) {
			sql.append(",");
			sql.append(nmColunaRetorno).append(" = '").append(cdRetorno).append("'");
		}
		if (dsMensagemErp != null) {
			sql.append(",");
			sql.append(NM_COLUNA_DSCONTROLE_ERP).append(" = '").append(dsMensagemErp).append("'");
		}
		sql.append(" WHERE ");
		for (Column column : pkList) {
			String columnName = column.getColumnName();
			sql.append(columnName).append(" = ?");
			sql.append(" AND ");
		}
		ps = jdbcTemplate.getDataSource().getConnection().prepareStatement(sql.substring(0, sql.length() - 4));
		
		int count = 0;
		for (Column column : pkList) {
			String columnName = column.getColumnName();
			sql.append(columnName).append(" = ").append(map.get(columnName));
			StatementCreatorUtils.setParameterValue(ps, count + 1, column.getDataType(), map.get(columnName));
			count++;
		}
		
		return ps;
	}
	
	protected void acionaProcedureDados2Erp(Object cdEmpresa, Object cdRepresentante) {
		boolean addHttp = ! urlProcedureDados2Erp.startsWith("http");
		boolean addSeparator = ! urlProcedureDados2Erp.endsWith("/");
		StringBuilder url = new StringBuilder();  
		if (addHttp) {
			url.append("http://");
		}
		url.append(urlProcedureDados2Erp);
		if (addSeparator) {
			url.append("/");
		}
		url.append(cdEmpresa + "/" + cdRepresentante);
		log.info("Acionando procedure dados2erp " + url);
		String response = "";
		try {
			HttpResponse<String> req = WebServiceUtil.unirestHttpResponseGet(url.toString());
			response = req.getBody();	
			log.info("Retorno da procedure " + response);
		} catch (Exception e) {
			log.error("Ocorreu um erro no acionamento da procedure dados2erp.", e);
		}
	}
	
}
