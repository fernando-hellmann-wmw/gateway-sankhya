package br.com.wmw.gatewaysankhya.integration;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.codec.binary.Base64;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import br.com.wmw.framework.util.ValueUtil;
import br.com.wmw.framework.util.Exception.UnexpectedException;
import br.com.wmw.framework.util.database.metadata.Column;

@Service
public class ImportaDadosGatewaySankhya {


	@Inject
	private LoginGatewaySankhya loginGatewaySankhya;
	@Value("${url.importacao:}")
	private String url;
	@Value("${token:}")
	private String token;
	@Value("${appkey:}")
	private String appkey;
	private final static String contentType = "Content-Type";
	private final static String tokenHeader = "token";
	private final static String appkeyHeader = "appkey";
	private final static String authorization = "Authorization";
	private final static String bearer = "Bearer ";
	private final static String applicationJson = "application/json";
	
	private static RestTemplate restTemplate = new RestTemplate();
	
	public String importaDados(String loginToken, String dsServico, List<Column> columnList, String dsFiltrosJson, int offSet) {
		return importaDados(loginToken, dsServico, columnList, dsFiltrosJson, offSet, true);
	}
	public String importaDados(String loginToken, String dsServico, List<Column> columnList, String dsFiltrosJson, int offSet, boolean tryNewLogin) {
		HttpHeaders headerPost = new HttpHeaders();
		headerPost.set(contentType, applicationJson);
		headerPost.set(tokenHeader, getParamDecode(token));
		headerPost.set(appkeyHeader, getParamDecode(appkey));
		headerPost.set(authorization, bearer + loginToken);
		String body = getRequestBody(dsServico, columnList, dsFiltrosJson, offSet);
		HttpEntity<String> postRequest = new HttpEntity<>(body, headerPost);
		try {
			ResponseEntity<String> result = restTemplate.postForEntity(url, postRequest, String.class);
			return result.getBody();
		 } catch (HttpClientErrorException e) {
			 if (e.getStatusCode().value() == 401 && tryNewLogin) {
				 String loginTokenNew = loginGatewaySankhya.login();
				 return importaDados(loginTokenNew, dsServico, columnList, dsFiltrosJson, offSet, false);
			 } else {
				 throw new UnexpectedException(e.getResponseBodyAsString());
			 }
	     }
	}

	private String getRequestBody(String dsApelido, List<Column> columnList, String dsFiltrosJson, int offSet) {
		StringBuilder body = new StringBuilder();
		body.append(" {" );
		body.append(" \"serviceName\": \"CRUDServiceProvider.loadRecords\", ");
		body.append(" \"requestBody\": { ");
		body.append(" \"dataSet\": { ");
		body.append(" \"rootEntity\": \"" + dsApelido + "\", ");
		body.append(" \"includePresentationFields\": \"S\", ");
		body.append(" \"offsetPage\": \"");
		body.append(offSet);
		body.append("\", ");
		body.append(" \"criteria\": { ");
		body.append(" \"expression\": {");
		if (ValueUtil.isNotEmpty(dsFiltrosJson)) {
			body.append(dsFiltrosJson);
		}
		body.append(" } ");
		body.append(" }, ");
		body.append("  \"entity\": { ");
		body.append(" \"fieldset\": { ");
		body.append(" \"list\": \"");
		for (Column column : columnList) {
			body.append(column.getColumnName());
			body.append(",");
		}
		body.deleteCharAt(body.length() - 1);
		body.append("\"");
		body.append(" }}}}} ");
		return body.toString();
	}

	private String getParamDecode(String param) {
		return new String(Base64.decodeBase64(param));
	}
}
