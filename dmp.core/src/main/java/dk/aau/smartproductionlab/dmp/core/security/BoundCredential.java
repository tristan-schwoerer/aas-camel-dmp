/*******************************************************************************
 * Copyright (C) 2026 the aas-camel-dmp Authors
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package dk.aau.smartproductionlab.dmp.core.security;

import java.util.Objects;
import java.util.UUID;

import org.apache.camel.CamelContext;
import dk.aau.smartproductionlab.dmp.core.routebuilder.ManagedRouteResource;

/**
 * Credentials kept out of endpoint URIs. They are bound in the Camel registry under an opaque name while the
 * owning bundle lives, and an endpoint URI only names them: {@code userName=#bean:<name>.user&password=#bean:<name>.password}.
 * Camel resolves the references when it creates the endpoint, so route definitions, endpoint registries and
 * diagnostics see the names, never the secret.
 */
public final class BoundCredential implements ManagedRouteResource {
	private final String name = "dmp-credential-" + UUID.randomUUID();
	private final String userName;
	private final String password;
	private CamelContext boundContext;
	private int owners;

	public BoundCredential(String userName, String password) {
		this.userName = Objects.requireNonNull(userName, "userName");
		this.password = password == null ? "" : password;
	}

	/** The opaque registry name; not a secret. */
	public String name() { return name; }
	public String userNameReference() { return "#bean:" + name + ".user"; }
	public String passwordReference() { return "#bean:" + name + ".password"; }

	/**
	 * A stable, non-secret identity of these credentials within this process: equal for equal credentials,
	 * so a connection cache keyed by it shares one session per server and user, and salted per process, so a
	 * URI that carries it reveals nothing about the password.
	 */
	public String connectionKey() {
		try {
			var digest = java.security.MessageDigest.getInstance("SHA-256");
			digest.update(SALT);
			digest.update((userName + '\0' + password).getBytes(java.nio.charset.StandardCharsets.UTF_8));
			return "dmp-" + java.util.HexFormat.of().formatHex(digest.digest(), 0, 12);
		} catch (java.security.NoSuchAlgorithmException impossible) {
			throw new IllegalStateException(impossible);
		}
	}

	private static final byte[] SALT = new java.security.SecureRandom().generateSeed(16);

	/** The bound user name, for components that cannot take a reference. */
	public String userName() { return userName; }
	public String password() { return password; }

	@Override public synchronized void bind(CamelContext context) {
		if (boundContext != null) {
			if (boundContext != context) throw new IllegalStateException("credential is bound in another CamelContext");
			owners++;
			return;
		}
		context.getRegistry().bind(name + ".user", userName);
		context.getRegistry().bind(name + ".password", password);
		boundContext = context;
		owners = 1;
	}

	@Override public synchronized void close() {
		if (boundContext == null) return;
		if (--owners > 0) return;
		boundContext.getRegistry().unbind(name + ".user");
		boundContext.getRegistry().unbind(name + ".password");
		boundContext = null;
	}

	@Override public String toString() { return "BoundCredential[" + name + ", <redacted>]"; }
}
