package br.com.wmw.gatewaysankhya.integration;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import br.com.wmw.framework.util.ValueUtil;
import br.com.wmw.framework.util.Exception.UnexpectedException;

@Service
public class LoginGatewaySankhya {

	private static final Log log = LogFactory.getLog(LoginGatewaySankhya.class);
	@Value("${url.login:}")
	private String urlLogin;
	@Value("${token:}")
	private String token;
	@Value("${appkey:}")
	private String appkey;
	@Value("${login.username:}")
	private String username;
	@Value("${login.password:}")
	private String password;
	private final static String bearerToken = "bearerToken";
	private final static String error = "error";
	private final static String status = "status";
	private final static String descricao = "descricao";
	private final static String codigo = "codigo";
	private static RestTemplate restTemplate = new RestTemplate();
	
	
	public String login() {
		ResponseEntity<String> result = executeLogin();
		if (result != null) {
			JSONObject loginJSon = new JSONObject(result.getBody());
			String loginToken = ValueUtil.toString(loginJSon.get(bearerToken));
			String errorMessage = ValueUtil.toString(loginJSon.get(error));
			if (ValueUtil.isNotEmpty(loginToken) && (errorMessage == null || ValueUtil.isEmpty(errorMessage) || ValueUtil.equals(errorMessage, "null"))) {
				return loginToken;
			}
		}
		return null;
	}
	
	
	private ResponseEntity<String> executeLogin() {
		log.info("Serviço de login executado no gateway sankhya.");
		log.debug("Url: " + urlLogin + " - Token: " + token + " - AppKey: " + appkey + " - Username: " + username + " - Password: " + password);
		HttpHeaders headerPost = new HttpHeaders();
		headerPost.set( "token", getParamDecode(token));
		headerPost.set( "appkey", getParamDecode(appkey));
		headerPost.set( "username", getParamDecode(username));
		headerPost.set( "password", getParamDecode(password));
		HttpEntity<String> postRequest = new HttpEntity<>(headerPost);
		try {
			return restTemplate.postForEntity(urlLogin, postRequest, String.class);
		 } catch(HttpClientErrorException e) {
			 if (ValueUtil.isNotEmpty(e.getResponseBodyAsString())) {
				 JSONObject jSonError = new JSONObject(e.getResponseBodyAsString());
				 trataErroLogin(jSonError);
				 return null;
			 }
			 throw new UnexpectedException(e);
	     }
	}
	
	private String getParamDecode(String param) {
		return new String(Base64.decodeBase64(param));
	}


	private void trataErroLogin(JSONObject jSon) {
		JSONObject errorObject = jSon.getJSONObject(error);
		if (errorObject != null) {
			String codigoErro = ValueUtil.toString(errorObject.get(codigo));
			String descricaoErro = ValueUtil.toString(errorObject.get(descricao));
			log.error("Erro ao realizar login no gateway sankhya. Código: " + codigoErro + " - Descrição: " + descricaoErro);
			return;
		}
		String statusErro = ValueUtil.toString(jSon.get(status));
		if (statusErro != null) {
			String errorMessage = ValueUtil.toString(jSon.get(error));
			log.error("Erro ao realizar login no gateway sankhya. Status: " + statusErro + " - Descrição: " + errorMessage);
			return;
		}
		log.error("Erro ao realizar login no gateway sankhya. " + jSon.toString());
	}
}
