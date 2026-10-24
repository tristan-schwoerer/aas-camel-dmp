package dk.aau.smartproductionlab.dmp.opcua.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.camel.component.milo.MiloConstants;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.StatusCode;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.junit.jupiter.api.Test;

class OpcUaSynchronousSourceReadAdapterTest {
	@Test
	void recognizesMiloEndpointsAndExtractsTheReadNode() {
		var adapter = new OpcUaSynchronousSourceReadAdapter();
		String uri = "milo-client:opc.tcp://localhost:4840/server?node=RAW(ns=2;s=Machine.State)";

		assertTrue(adapter.supports(uri));
		assertFalse(adapter.supports("paho-mqtt5:state?brokerUrl=tcp://localhost:1883"));
		assertEquals("ns=2;s=Machine.State", OpcUaSynchronousSourceReadAdapter.nodeIdFromEndpoint(uri));
	}

	@Test
	void aFreshReadIsTheVariablesValue() {
		var value = new DataValue(new Variant("READY"));

		assertEquals("READY", OpcUaSynchronousSourceReadAdapter
				.normalizeReadResult(List.of(value)));
	}

	@Test
	void requestsAnAwaitedMiloReadForTheSelectedNode() {
		var headers = OpcUaSynchronousSourceReadAdapter.readHeaders("ns=2;s=Machine.State");
		assertEquals(List.of("ns=2;s=Machine.State"), headers.get(MiloConstants.HEADER_NODE_IDS));
		assertEquals(Boolean.TRUE, headers.get(MiloConstants.HEADER_AWAIT));
	}

	@Test
	void awaitsMiloReadCompletionBeforeNormalizing() {
		var value = new DataValue(new Variant("READY"));
		var pending = new CompletableFuture<List<DataValue>>();
		var reader = CompletableFuture.supplyAsync(() -> OpcUaSynchronousSourceReadAdapter
				.normalizeReadResult(OpcUaSynchronousSourceReadAdapter.awaitReadResult(pending)));
		assertFalse(reader.isDone());
		pending.complete(List.of(value));
		assertEquals("READY", reader.join());
	}

	@Test
	void failedMiloReadCompletionDoesNotBecomeAContribution() {
		var failed = new CompletableFuture<Object>();
		failed.completeExceptionally(new IllegalStateException("read failed"));
		IllegalStateException failure = assertThrows(IllegalStateException.class,
				() -> OpcUaSynchronousSourceReadAdapter.awaitReadResult(failed));
		assertEquals("OPC UA Read failed", failure.getMessage());
	}

	@Test
	void absentAndBadStatusReadsNeverBecomeContributions() {
		assertThrows(IllegalStateException.class,
				() -> OpcUaSynchronousSourceReadAdapter.normalizeReadResult(null));
		assertThrows(IllegalStateException.class, () -> OpcUaSynchronousSourceReadAdapter.normalizeReadResult(
				List.of(new DataValue(new Variant("BAD"), StatusCode.BAD))));
	}

	@Test
	void aNodeIdWithParenthesesIsReadWhole() {
		assertEquals("ns=2;s=Pump(1).Speed", OpcUaSynchronousSourceReadAdapter.nodeIdFromEndpoint(
				"dmp-milo-client:opc.tcp://host:4840/?node=RAW(ns=2;s=Pump(1).Speed)&allowedSecurityPolicies=None"));
		assertEquals("ns=2;s=Pump(1)", OpcUaSynchronousSourceReadAdapter.nodeIdFromEndpoint(
				"dmp-milo-client:opc.tcp://host:4840/?node=RAW(ns=2;s=Pump(1))"));
		assertEquals("/Root/Objects/2:Pump(1)", OpcUaSynchronousSourceReadAdapter.nodeIdFromEndpoint(
				"dmp-milo-client:opc.tcp://host:4840/?browsePath=RAW(/Root/Objects/2:Pump(1))&requestTimeout=30000"));
	}
}
