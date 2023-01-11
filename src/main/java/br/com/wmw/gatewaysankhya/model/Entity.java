package br.com.wmw.gatewaysankhya.model;

import java.util.HashMap;
import java.util.List;

import com.google.gson.annotations.SerializedName;

public class Entity {

	private List<HashMap<String, Valor>> entity;

	public List<HashMap<String, Valor>> getEntity() {
		return entity;
	}

	public void setEntity(List<HashMap<String, Valor>> entity) {
		this.entity = entity;
	}

public	class Valor {

		@SerializedName("$")
		private String value;

		public String getValue() {
			return value;
		}

		public void setValue(String asd) {
			this.value = asd;
		}

	}

}
