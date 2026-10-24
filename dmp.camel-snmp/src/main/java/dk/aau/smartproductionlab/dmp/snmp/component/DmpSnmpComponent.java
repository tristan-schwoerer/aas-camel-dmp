/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.snmp.component;

import java.util.Locale;
import java.util.Map;

import org.apache.camel.Endpoint;
import org.apache.camel.support.DefaultComponent;
import org.snmp4j.mp.SnmpConstants;

/**
 * {@code dmp-snmp:host:port?oid=…}: one SNMP v1/v2c object (AID 1.2 SNMP binding). A read is a
 * GET, a write a SET of the object's own SMI type; an error status or a missing object raises.
 * camel-snmp answers a GET without checking it and has no SET, so the DMP ships this component
 * (SPEC §9).
 */
public final class DmpSnmpComponent extends DefaultComponent {
	public static final String SCHEME = "dmp-snmp";

	/**
	 * The endpoint URI of one object. The community is SNMP's credential: the DMP passes a registry reference
	 * ({@code #bean:<name>}), never the value (SPEC §11); without one the community is {@code public}.
	 */
	public static String uri(String host, int port, String oid, String version, String communityReference) {
		StringBuilder uri = new StringBuilder(SCHEME).append(':').append(host).append(':').append(port)
				.append("?oid=").append(oid).append("&version=").append(version);
		if (communityReference != null) uri.append("&community=").append(communityReference);
		return uri.toString();
	}

	@Override
	protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) throws Exception {
		int colon = remaining.lastIndexOf(':');
		if (colon <= 0 || colon == remaining.length() - 1)
			throw new IllegalArgumentException("dmp-snmp endpoint must name host:port, not " + remaining);
		String host = remaining.substring(0, colon);
		int port = Integer.parseInt(remaining.substring(colon + 1));
		String oid = getAndRemoveParameter(parameters, "oid", String.class);
		if (oid == null || oid.isBlank()) throw new IllegalArgumentException("dmp-snmp endpoint requires an oid");
		int version = switch (getAndRemoveParameter(parameters, "version", String.class, "SNMPv2c").toLowerCase(Locale.ROOT)) {
			case "snmpv1", "1", "v1" -> SnmpConstants.version1;
			case "snmpv2c", "2c", "v2c" -> SnmpConstants.version2c;
			default -> throw new IllegalArgumentException("dmp-snmp supports SNMPv1 and SNMPv2c");
		};
		String community = getAndRemoveOrResolveReferenceParameter(parameters, "community", String.class, "public");
		long timeout = getAndRemoveParameter(parameters, "timeout", Long.class, DmpSnmpEndpoint.DEFAULT_TIMEOUT_MS);
		int retries = getAndRemoveParameter(parameters, "retries", Integer.class, 1);
		return new DmpSnmpEndpoint(uri, this, host, port, oid, version, community, timeout, retries);
	}
}
