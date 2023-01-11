package br.com.wmw.gatewaysankhya.service;

import java.lang.reflect.Type;
import java.sql.Date;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.httpclient.HttpStatus;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;

public class PersistResult {

	private Map<String, Map<String, ResultUnit>> results = new LinkedHashMap<>();
	private Type mapType = new TypeToken<Map<String, ResultUnit>>(){}.getType(); 
	private Date sendDate;
	private String sendTime;
	private boolean persistSuccessOccurred;
	private static final String SUCCESS = "success";
	private static final String ERROR = "error";
	
	public PersistResult(Date sendDate, String sendTime) {
		this.sendDate = sendDate;
		this.sendTime = sendTime;
		this.results.put(SUCCESS, new LinkedHashMap<>());
		this.results.put(ERROR, new LinkedHashMap<>());
	}
	
	public void addSuccess(final String id, final int statusCode, final String msg) {
		results.get(SUCCESS).put(id, new ResultUnit(statusCode, msg));
		persistSuccessOccurred |= HttpStatus.SC_OK == statusCode;
	}
	
	public void addError(final String id, final int statusCode, final String msg) {
		results.get(ERROR).put(id, new ResultUnit(statusCode, msg));
	}
	
	public Map<String, Map<String, ResultUnit>> getRawResult() {
		return results;
	}
	
	public String asJson() {
		if (results.isEmpty()) {
			return new JsonObject().toString();
		}
		return new Gson().toJson(results, mapType);
	}
	
	public Date getSendDate() {
		return sendDate;
	}
	
	public String getSendTime() {
		return sendTime;
	}
	
	public boolean persistOccurred() {
		return persistSuccessOccurred;
	}
	
	private class ResultUnit {
		
		private int statusCode;
		private String message;
		
		public ResultUnit(int statusCode, String msg) {
			this.statusCode = statusCode;
			this.message = msg;
		}
		
		@Override
		public String toString() {
			return new Gson().toJson(this, ResultUnit.class);
		}
		
	}
	
}
