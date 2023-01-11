package br.com.wmw.gatewaysankhya.service;

import java.sql.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import br.com.wmw.framework.integrator.data.RdbMetadata;
import br.com.wmw.framework.integrator.data.Row;
import br.com.wmw.framework.integrator.util.RdbUtil;
import br.com.wmw.framework.util.DateUtil;
import br.com.wmw.framework.util.ValueUtil;
import br.com.wmw.framework.util.database.metadata.Column;
import br.com.wmw.framework.util.database.metadata.Table;
import br.com.wmw.gatewaysankhya.model.Entity.Valor;
import br.com.wmw.gatewaysankhya.model.Metadata;

public class InsertSql {
	
	private RdbMetadata rdbMetadata;
	private Table table;
	private List<Column> columns;
	private Metadata metadata;
	
	private String sql;
	private Map<String, Row> dataRows;
	
	private Date startDate;
	private String startTime;
	
	private InsertSql(Builder builder) {
		startDate = new Date(DateUtil.getDataAtual().getTime());
		startTime = DateUtil.getHoraAtualAsString();
		table = builder.table;
		metadata = builder.metadata;
		columns = table.getColumns();
		buildInsertSql();
		rdbMetadata = new RdbMetadata(table, columns);
		buildRows(builder.entity);
	}

	private void buildRows(List<HashMap<String, Valor>> entity) {
		dataRows = new LinkedHashMap<>();
		int count = 0;
		for (HashMap<String, Valor> hashMap : entity) {
			Row row = new Row();
			row.setMetadata(rdbMetadata);
			Map<String, Column> columnMap = table.getColumnMap();
			for (Entry<String, Valor> hashValor : hashMap.entrySet()) {
				String index = hashValor.getKey().substring(1);
				HashMap<String, Object> column = metadata.getField().get(ValueUtil.toInteger(index));
				String fieldName = (String) column.get("name");
				if (columnMap.containsKey(fieldName)) {
					row.setValue(fieldName, hashValor.getValue().getValue());
				}
			}
			dataRows.put(ValueUtil.toString(count), row);
			count++;
		}
	}

	public Map<String, Row> getDataRows() {
		return dataRows;
	}

	private void buildInsertSql() {
		sql = RdbUtil.getInsertSQL(new RdbMetadata(table, table.getColumns()), false, false);
	}
	
	@Override
	public String toString() {
		return String.valueOf(sql).toLowerCase().trim();
	}
	
	public Table getTable() {
		return table;
	}
	
	public Date getInsertDate() {
		return startDate;
	}
	
	public String getInsertTime() {
		return startTime;
	}
	
	public static class Builder {
		
		private Table table;
		private List<HashMap<String, Valor>> entity;
		private Metadata metadata;
		
		public Builder(Table table, Metadata metadata) {
			this.table = table;
			this.metadata = metadata;
		}
		
		public InsertSql build() {
			return new InsertSql(this);
		}

		public Builder setEntity(List<HashMap<String, Valor>> entity) {
			this.entity = entity;
			return this;
		}

	}

}
