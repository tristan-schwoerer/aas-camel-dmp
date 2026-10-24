/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.snmp.configuration;

import dk.aau.smartproductionlab.dmp.core.plan.BundleResources;
import dk.aau.smartproductionlab.dmp.core.plan.Endpoint;
import dk.aau.smartproductionlab.dmp.core.security.BoundCredential;
import dk.aau.smartproductionlab.dmp.snmp.component.DmpSnmpComponent;

/**
 * An SNMP object, read by polling (GET) as a source and written with SET as a sink. The community, SNMP's
 * credential, is bound as the user of a {@link BoundCredential} without password; {@code null} for none.
 */
public record SnmpEndpoint(String host, int port, String oid, String version, String community) implements Endpoint {
	@Override
	public String uri(BundleResources resources) {
		BoundCredential credential = community == null ? null : resources.own(new BoundCredential(community, ""));
		return DmpSnmpComponent.uri(host, port, oid, version, credential == null ? null : credential.userNameReference());
	}

	@Override public String toString() {
		return "SnmpEndpoint[" + host + ":" + port + "/" + oid + ", " + version + ", <community redacted>]";
	}
}
