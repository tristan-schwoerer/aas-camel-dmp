/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.snmp.configuration;

import org.apache.camel.CamelContext;

import dk.aau.smartproductionlab.dmp.core.configuration.entity.ManagedRouteResource;
import dk.aau.smartproductionlab.dmp.core.configuration.entity.DataSinkConfiguration;
import dk.aau.smartproductionlab.dmp.core.security.BoundCredential;
import dk.aau.smartproductionlab.dmp.snmp.component.DmpSnmpComponent;

/** An SNMP object written with SET. */
public class SnmpDataSinkConfiguration extends DataSinkConfiguration implements ManagedRouteResource {
	private String host;
	private int port;
	private String oid;
	private String version;
	private String community;
	private BoundCredential bound;

	public SnmpDataSinkConfiguration() {}

	public SnmpDataSinkConfiguration(String uniqueId, String host, int port, String oid, String version, String community) {
		super(uniqueId);
		this.host = host;
		this.port = port;
		this.oid = oid;
		this.version = version;
		this.community = community;
	}

	public String getOid() { return oid; }

	@Override
	public String getConnectionURI() {
		BoundCredential credential = boundCommunity();
		return DmpSnmpComponent.uri(host, port, oid, version, credential == null ? null : credential.userNameReference());
	}

	/** The community, SNMP's credential, bound as the user of a {@link BoundCredential} without password. */
	private synchronized BoundCredential boundCommunity() {
		if (bound == null && community != null) bound = new BoundCredential(community, "");
		return bound;
	}

	@Override
	public void bind(CamelContext context) {
		BoundCredential credential = boundCommunity();
		if (credential != null) credential.bind(context);
	}

	@Override
	public synchronized void close() {
		if (bound != null) bound.close();
	}
}
