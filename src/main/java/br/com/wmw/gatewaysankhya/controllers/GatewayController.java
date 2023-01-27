package br.com.wmw.gatewaysankhya.controllers;


import java.util.Arrays;
import java.util.Date;
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
import br.com.wmw.gatewaysankhya.integration.EntidadeGenericaToGatewaySankhya;
import br.com.wmw.gatewaysankhya.integration.NovoClienteToGatewaySankhya;
import br.com.wmw.gatewaysankhya.integration.PedidoToGatewaySankhya;
import br.com.wmw.gatewaysankhya.integration.SankhyaIntegrator;


@Controller
@RequestMapping("gatewaysankhya")
public class GatewayController {

	private static final Logger log = LoggerFactory.getLogger(GatewayController.class);
	private static final String TODAS_ENTIDADES = "TODAS";
	
	@Inject 
	private SankhyaIntegrator sankhyaIntegrator;
	@Inject 
	private PedidoToGatewaySankhya pedidoToGatewaySankhya;
	@Inject 
	private NovoClienteToGatewaySankhya novoClienteToGatewaySankhya;
	@Inject
	private EntidadeGenericaToGatewaySankhya entidadeGenericaToGatewaySankhya;
	
	@SuppressWarnings("rawtypes")
	@GetMapping(value = "/echo")
	public ResponseEntity echo() {
		return ResponseEntity.status(HttpStatus.OK).body("Echo " + new Date());
	}
	
	@SuppressWarnings("rawtypes")
	@ResponseBody
	@GetMapping(value = "/importadados")
	public ResponseEntity importaDados() {
		log.info("Acionada importação de dados.");
		sankhyaIntegrator.execute();
		log.info("Finalizada importação de dados.");
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
		log.info("Finalizada importação parcial de dados para as entidades " + entidades);
		return ResponseEntity.status(HttpStatus.OK).body("Importação finalizada com sucesso!");
	}

	@SuppressWarnings("rawtypes")
	@ResponseBody
	@GetMapping(value = "/enviapedido")
	public ResponseEntity enviaPedido() {
		log.info("Acionado envio de pedidos para o gateway sankhya.");
		pedidoToGatewaySankhya.execute();
		log.info("Finalizado envio de pedidos para o gateway sankhya.");
		return ResponseEntity.status(HttpStatus.OK).body("Envio de pedidos finalizado com sucesso!");
	}

	@SuppressWarnings("rawtypes")
	@ResponseBody
	@GetMapping(value = "/envianovocliente")
	public ResponseEntity enviaNovoCliente() {
		log.info("Acionado envio de novos clientes para o gateway sankhya.");
		novoClienteToGatewaySankhya.execute();
		log.info("Finalizado envio de novos clientes para o gateway sankhya.");
		return ResponseEntity.status(HttpStatus.OK).body("Envio de novos clientes finalizado com sucesso!");
	}

	@SuppressWarnings("rawtypes")
	@ResponseBody
	@GetMapping(value = "/envia/{entidades}")
	public ResponseEntity envia(@PathVariable String entidades) {
		if (ValueUtil.isEmpty(entidades)) {
			return ResponseEntity.status(HttpStatus.OK).body("Nenhuma entidade informada para o envio de dados!");
		}
		log.info("Acionado envio das entidades " + entidades + " para o gateway sankhya.");
		if (TODAS_ENTIDADES.equalsIgnoreCase(entidades)) {
			entidadeGenericaToGatewaySankhya.execute();
		} else {
			List<String> entidadeList = Arrays.asList(entidades.split(","));
			entidadeGenericaToGatewaySankhya.execute(entidadeList);
		}
		log.info("Finalizado envio das entidades " + entidades + " para o gateway sankhya.");
		return ResponseEntity.status(HttpStatus.OK).body("Envio de entidades finalizado com sucesso!");
	}
}
