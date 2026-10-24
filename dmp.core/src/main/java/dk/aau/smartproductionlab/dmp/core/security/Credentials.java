/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.core.security;

import java.util.Objects;

import dk.aau.smartproductionlab.dmp.core.plan.BundleResources;

/**
 * A user name and password as a plan carries them. Never printed; an endpoint URI only names them once
 * {@link #bind bound}.
 */
public record Credentials(String userName, String password) {
	public Credentials {
		Objects.requireNonNull(userName, "userName");
		if (password == null) password = "";
	}

	/** The credentials bound in {@code resources} for the bundle's lifetime. */
	public BoundCredential bind(BundleResources resources) {
		return resources.own(new BoundCredential(userName, password));
	}

	/** Credentials for {@code userName}, or {@code null} when it is null or blank. */
	public static Credentials of(String userName, String password) {
		return userName == null || userName.isBlank() ? null : new Credentials(userName, password);
	}

	@Override public String toString() { return "Credentials[<redacted>]"; }
}
