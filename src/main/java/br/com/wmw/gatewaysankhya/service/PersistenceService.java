package br.com.wmw.gatewaysankhya.service;

import javax.inject.Inject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;

import br.com.wmw.framework.util.ValueUtil;
import br.com.wmw.framework.util.database.metadata.MetadataUtil;
import br.com.wmw.framework.util.database.metadata.Table;
import br.com.wmw.gatewaysankhya.model.Entity;
import br.com.wmw.gatewaysankhya.model.Metadata;

@Service
public class PersistenceService {

	private static Log log = LogFactory.getLog(PersistenceService.class);
	
	private static final String resposeBodyTag = "responseBody";
	private static final String entitiesTag = "entities";
	private static final String entityTag = "entity";
	private static final String metadataTag = "metadata";
	private static final String fieldsTag = "fields";
	private static final String statusSucesso = "1";
	private static final String hasMoreResultsTag = "hasMoreResult";
	private static final String totalTag = "total";
	private static final String statusTag = "status";	
	private static final String statusMessageTag = "statusMessage";
	
	@Inject
	JdbcTemplate jdbcTemplate = new JdbcTemplate();
	
	@Value("${import.table.prefix:}")
	private String importTablePrefix;

	
	public boolean persisteDados(String nmEntidade, String jsonData) {
		JSONObject json = new JSONObject(jsonData);
		String status = json.get(statusTag).toString();
		if (! statusSucesso.equals(status)) {
			String statusMessage = json.get(statusMessageTag).toString();
			log.error("Houve um erro na importação da entidade " + nmEntidade + ". " + statusMessage);
			return false;
		}
		json = json.getJSONObject(resposeBodyTag).getJSONObject(entitiesTag);
		String qtRegistros = json.get(totalTag).toString();
		boolean hasMoreResults = json.getBoolean(hasMoreResultsTag);
		if (ValueUtil.toInteger(qtRegistros) == null || ValueUtil.toInteger(qtRegistros) == 0) {
			log.info("Nenhum registro encontrado para importação da tabela " + nmEntidade);
			return false;
		}
		StringBuilder jSonEntity = null;
		if (ValueUtil.toInteger(qtRegistros) > 1) {
			jSonEntity = new StringBuilder(json.getJSONArray(entityTag).toString());
			jSonEntity.insert(0, "{\"" + entityTag +"\":");
		} else {
			jSonEntity = new StringBuilder(json.getJSONObject(entityTag).toString());
			jSonEntity.insert(0, "{\"" + entityTag +"\":[");
			jSonEntity.insert(jSonEntity.length(), "]");
		}
		jSonEntity.insert(jSonEntity.length(), "}");
		Entity entity = new Gson().fromJson(jSonEntity.toString(), Entity.class);
		StringBuilder jSonMetadata = new StringBuilder(json.getJSONObject(metadataTag).getJSONObject(fieldsTag).toString());
		Metadata metadata = new Gson().fromJson(jSonMetadata.toString(), Metadata.class);
		Table table = MetadataUtil.extractTableMetadata(jdbcTemplate.getDataSource(), importTablePrefix + nmEntidade);
		InsertSql entrySql = new InsertSql.Builder(table, metadata)
				.setEntity(entity.getEntity())
				.build();
		PersistResult result = new PersistResult(entrySql.getInsertDate(), entrySql.getInsertTime());
		SaveDataWriter saveDataWriter = new SaveDataWriter(jdbcTemplate.getDataSource(), entrySql, result);
		saveDataWriter.push();
		log.info(result.asJson());
		return hasMoreResults;
	}

}
