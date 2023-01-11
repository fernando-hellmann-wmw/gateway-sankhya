package br.com.wmw.gatewaysankhya.model;

import java.util.LinkedHashMap;
import java.util.List;

public class Metadata {

	private List<LinkedHashMap<String, Object>> field;

	public List<LinkedHashMap<String, Object>> getField() {
		return field;
	}

	public void setField(List<LinkedHashMap<String, Object>> field) {
		this.field = field;
	}
	
}
