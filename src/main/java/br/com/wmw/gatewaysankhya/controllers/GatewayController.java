package br.com.wmw.gatewaysankhya.controllers;


import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import br.com.wmw.framework.util.ValueUtil;
import br.com.wmw.gatewaysankhya.integration.PedidoToGatewaySankhya;
import br.com.wmw.gatewaysankhya.integration.SankhyaIntegrator;


@Controller
@RequestMapping("gatewaysankhya")
public class GatewayController {

	private static final Logger log = LoggerFactory.getLogger(GatewayController.class);
	
	@Inject 
	private SankhyaIntegrator sankhyaIntegrator;
	@Inject 
	private PedidoToGatewaySankhya pedidoToGatewaySankhya;
	
	@SuppressWarnings("rawtypes")
	@ResponseBody
	@GetMapping(value = "/importadados")
	public ResponseEntity importaDados() {
		log.info("Acionada importação de dados.");
		sankhyaIntegrator.execute();
		return ResponseEntity.status(HttpStatus.OK).body("Importação finalizada com sucesso!");
	}
	
	@SuppressWarnings("rawtypes")
	@ResponseBody
	@GetMapping(value = "/importadadosparcial/{entidades}")
	public ResponseEntity importaDadosParcial(@PathVariable String entidades) {
		log.info("Acionada importação parcial de dados para as entidades " + entidades);
		if (ValueUtil.isEmpty(entidades)) {
			log.warn("Nenhuma entidade enviada por parâmetro!");
			return ResponseEntity.status(HttpStatus.OK).body("Integração finalizada");
		}
		List<String> entidadeList = Arrays.asList(entidades.split(","));
		sankhyaIntegrator.execute(entidadeList);
		return ResponseEntity.status(HttpStatus.OK).body("Importação finalizada com sucesso!");
	}

	@SuppressWarnings("rawtypes")
	@ResponseBody
	@GetMapping(value = "/enviapedido")
	public ResponseEntity enviaPedido() {
		log.info("Acionado envio de pedidos para o gateway sankhya.");
		pedidoToGatewaySankhya.execute();
		return ResponseEntity.status(HttpStatus.OK).body("Envio de pedidos finalizado com sucesso!");
	}
}
