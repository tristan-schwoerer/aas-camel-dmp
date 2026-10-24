/*******************************************************************************
 * Copyright (C) 2026 the aas-camel-dmp Authors
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package dk.aau.smartproductionlab.dmp.core.routebuilder;

import org.apache.camel.CamelContext;

/** A configuration-owned resource whose lifetime is bounded by a managed route bundle. */
public interface ManagedRouteResource extends AutoCloseable {
	void bind(CamelContext context) throws Exception;

	/** Drain accepted detached work while its downstream routes are still available. */
	default void drain(java.time.Duration timeout) throws Exception { }

	/** Exact lazy endpoints which must retire with this resource's bundle. */
	default java.util.Set<String> endpointUris() { return java.util.Set.of(); }

	/** Nonblocking aggregate counters only; never resource identities or transport settings. */
	default java.util.Map<String, Long> metrics() { return java.util.Map.of(); }

	/**
	 * Identity-deduplicated views of resources retained by this configuration.
	 * Byte counters describe exact retained payload lengths, not object overhead,
	 * compiled representations, JVM heap estimates or heap isolation. Implementations
	 * must read cached/local state without I/O or lifecycle/transport locks.
	 */
	default java.util.List<AccountingEntry> accounting() {
		return java.util.List.of(new AccountingEntry(this, metrics()));
	}

	/** Optional endpoint SPI; observing it must never create a producer or connection. */
	interface AccountedEndpoint {
		java.util.List<AccountingEntry> accounting();
	}

	/** Identity is an opaque in-process deduplication token and must not be exported. */
	record AccountingEntry(Object identity, java.util.Map<String, Long> counters) {
		public AccountingEntry {
			java.util.Objects.requireNonNull(identity, "identity");
			counters = java.util.Map.copyOf(counters);
		}
	}

	@Override
	void close() throws Exception;
}
