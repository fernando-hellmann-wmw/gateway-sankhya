package br.com.wmw.gatewaysankhya;

import java.util.HashMap;
import java.util.Map.Entry;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import com.google.gson.Gson;

import br.com.wmw.gatewaysankhya.model.Entity;
import br.com.wmw.gatewaysankhya.model.Metadata;
import br.com.wmw.gatewaysankhya.model.Entity.Valor;

@SpringBootTest
class GatewaysankyaApplicationTests {

	@Test
	void contextLoads() {
		String asd = "false";
		String s = """
				},
				       "itens":{
				          "INFORMARPRECO":" """ + asd + """
",
				          "item":[
				""";
		s.toString();
		Entity ent =	new Gson().fromJson("{\"entity\": [\n"
				+ "				{\n"
				+ "					\"f6\": {\n"
				+ "						\"$\": \"17/01/2022\"\n"
				+ "					},\n"
				+ "					\"f0\": {\n"
				+ "						\"$\": \"8\"\n"
				+ "					},\n"
				+ "					\"f1\": {\n"
				+ "						\"$\": \"14/01/2022\"\n"
				+ "					},\n"
				+ "					\"f2\": {},\n"
				+ "					\"f3\": {},\n"
				+ "					\"f4\": {\n"
				+ "						\"$\": \"0\"\n"
				+ "					},\n"
				+ "					\"f5\": {\n"
				+ "						\"$\": \"0\"\n"
				+ "					}\n"
				+ "				}]}", Entity.class);
		for (HashMap<String, Valor> hashMap : ent.getEntity()) {
			for (Entry<String, Valor> valor : hashMap.entrySet()) {
				valor.getKey();
			}
		}
	}

	@Test
	void testMetadata() {
		String asd = "{\"field\": [\n"
				+ "	{\n"
				+ "		\"name\": \"NUTAB\"\n"
				+ "	},\n"
				+ "	{\n"
				+ "		\"name\": \"DTVIGOR\"\n"
				+ "	},\n"
				+ "	{\n"
				+ "		\"name\": \"PERCENTUAL\"\n"
				+ "	},\n"
				+ "	{\n"
				+ "		\"name\": \"UTILIZADECCUSTO\"\n"
				+ "	},\n"
				+ "	{\n"
				+ "		\"name\": \"CODTABORIG\"\n"
				+ "	},\n"
				+ "	{\n"
				+ "		\"name\": \"CODTAB\"\n"
				+ "	},\n"
				+ "	{\n"
				+ "		\"name\": \"DTALTER\"\n"
				+ "	}\n"
				+ "]\n"
				+ "}";
		Metadata metadata = new Gson().fromJson(asd, Metadata.class);
		metadata.toString();
	}
	
}
