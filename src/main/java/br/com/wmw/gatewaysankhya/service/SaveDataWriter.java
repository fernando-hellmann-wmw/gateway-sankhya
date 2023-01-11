package br.com.wmw.gatewaysankhya.service;

import java.util.Map;
import java.util.Map.Entry;

import javax.sql.DataSource;

import org.apache.commons.httpclient.HttpStatus;

import br.com.wmw.framework.integrator.data.RdbMetadata;
import br.com.wmw.framework.integrator.data.Row;
import br.com.wmw.framework.integrator.util.SqlType;
import br.com.wmw.framework.integrator.writer.AbstractDbDataWriter;
import br.com.wmw.framework.util.ValueUtil;
import br.com.wmw.framework.util.database.metadata.Table;

public class SaveDataWriter extends AbstractDbDataWriter {
	
	private static final int BATCH_SIZE = 1;
	private InsertSql insertSql;
	
	private PersistResult persistResult;
	
	public SaveDataWriter(DataSource dataSource, InsertSql insertSql, PersistResult persistResult) {
		this(dataSource, insertSql.getTable(), SqlType.INSERT, BATCH_SIZE, new RdbMetadata(insertSql.getTable(), insertSql.getTable().getColumns()));
		this.insertSql = insertSql;
		this.persistResult = persistResult;
	}
	
	public SaveDataWriter(DataSource dataSource, Table table, SqlType sqlType, int batchSize, RdbMetadata rdbMetadata) {
		super(dataSource, table, sqlType, batchSize, rdbMetadata);
	}

	@Override
	protected StringBuilder getSql() {
		return new StringBuilder(insertSql.toString());
	}
	
	protected void push() {
		Map<String, Row> rowList = insertSql.getDataRows();
		if (ValueUtil.isEmpty(rowList)) {
			return  ;
		}
		beginWrite();
		try {
			for (Entry<String, Row> rowPair : rowList.entrySet()) {
				writeRow(rowPair);
			}
		} finally {
			endWrite();
		}
	}

	private void writeRow(Entry<String, Row> rowPair) {
		String id = rowPair.getKey();
		try {
			write(rowPair.getValue());
			persistResult.addSuccess(rowPair.getKey(), HttpStatus.SC_OK, "Registro inserido com sucesso.");
		} catch (Exception e) {
			persistResult.addError(id, HttpStatus.SC_BAD_REQUEST, "Ocorreu um erro ao inserir o item. " + e.getMessage());
		}
	}
	
}
