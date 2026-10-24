/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.snmp.testing;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.snmp4j.CommandResponder;
import org.snmp4j.CommandResponderEvent;
import org.snmp4j.PDU;
import org.snmp4j.Snmp;
import org.snmp4j.mp.StatusInformation;
import org.snmp4j.smi.Null;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.UdpAddress;
import org.snmp4j.smi.Variable;
import org.snmp4j.smi.VariableBinding;
import org.snmp4j.transport.DefaultUdpTransportMapping;

/** A minimal SNMP v1/v2c agent on 127.0.0.1: GET and SET on a map of objects, some read-only. */
public final class SnmpTestAgent implements AutoCloseable {
	private final Map<OID, Variable> objects = new ConcurrentHashMap<>();
	private final Set<OID> readOnly = ConcurrentHashMap.newKeySet();
	private final Snmp snmp;
	private final int port;
	private volatile String lastCommunity;

	public SnmpTestAgent() throws IOException {
		DefaultUdpTransportMapping transport = new DefaultUdpTransportMapping(new UdpAddress("127.0.0.1/0"));
		snmp = new Snmp(transport);
		snmp.addCommandResponder(new CommandResponder() {
			@Override
			public <A extends org.snmp4j.smi.Address> void processPdu(CommandResponderEvent<A> event) {
				lastCommunity = new String(event.getSecurityName(), java.nio.charset.StandardCharsets.UTF_8);
				respond(event);
			}
		});
		transport.listen();
		port = transport.getListenAddress().getPort();
	}

	public int port() { return port; }

	/** The community of the last request. */
	public String lastCommunity() { return lastCommunity; }

	public SnmpTestAgent put(String oid, Variable value, boolean writable) {
		objects.put(new OID(oid), value);
		if (!writable) readOnly.add(new OID(oid));
		return this;
	}

	public Variable get(String oid) { return objects.get(new OID(oid)); }

	private <A extends org.snmp4j.smi.Address> void respond(CommandResponderEvent<A> event) {
		PDU request = event.getPDU();
		if (request == null) return;
		PDU response = (PDU) request.clone();
		response.setType(PDU.RESPONSE);
		for (int i = 0; i < response.size(); i++) {
			VariableBinding binding = response.get(i);
			Variable current = objects.get(binding.getOid());
			if (request.getType() == PDU.SET) {
				if (current == null || readOnly.contains(binding.getOid())) {
					response.setErrorStatus(current == null ? PDU.noCreation : PDU.notWritable);
					response.setErrorIndex(i + 1);
				} else if (current.getSyntax() != binding.getVariable().getSyntax()) {
					response.setErrorStatus(PDU.wrongType);
					response.setErrorIndex(i + 1);
				} else {
					objects.put(binding.getOid(), binding.getVariable());
				}
			} else {
				binding.setVariable(current == null ? Null.noSuchObject : current);
			}
		}
		try {
			event.getMessageDispatcher().returnResponsePdu(event.getMessageProcessingModel(), event.getSecurityModel(),
					event.getSecurityName(), event.getSecurityLevel(), response, event.getMaxSizeResponsePDU(),
					event.getStateReference(), new StatusInformation());
		} catch (Exception failure) {
			throw new IllegalStateException(failure);
		}
		event.setProcessed(true);
	}

	@Override
	public void close() throws IOException {
		snmp.close();
	}
}
