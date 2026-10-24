/*******************************************************************************
 * Copyright (C) 2026 the Eclipse BaSyx Authors
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
package dk.aau.smartproductionlab.dmp.lua.configuration;

import java.util.UUID;

import org.apache.camel.CamelContext;
import dk.aau.smartproductionlab.dmp.core.routebuilder.ManagedRouteResource;
import dk.aau.smartproductionlab.dmp.lua.sandbox.LuaCompiledSource;

/**
 * A Lua blob compiled in memory from the AIMC, bound in the owning Camel registry while its bundle lives and
 * referenced as {@code lua:managed:<binding>?timeoutSeconds=...} ({@link #uri()}); the endpoint transports decoded
 * Java values keyed by SourceId. The blob must expose {@code function aimc_main(sources)}.
 *
 * @author aas-camel-dmp
 */
public class LuaBinding implements ManagedRouteResource {

	public static final String DEFAULT_TIMEOUT_SECONDS = "2";

	private final String bindingName = "lua-source-" + UUID.randomUUID();
	private String timeoutSeconds = DEFAULT_TIMEOUT_SECONDS;
	private volatile LuaCompiledSource managedSource;
	private CamelContext boundContext;
	private volatile int bindingOwners;

	@Override public java.util.Map<String, Long> metrics() {
		return java.util.Map.of("luaBindings", bindingOwners > 0 ? 1L : 0L);
	}

	@Override public java.util.List<AccountingEntry> accounting() {
		LuaCompiledSource source = managedSource;
		AccountingEntry bindings = new AccountingEntry(this, metrics());
		return source == null ? java.util.List.of(bindings) : java.util.List.of(bindings,
				new AccountingEntry(source, java.util.Map.of("luaSourceRetainedBytes", source.retainedSourceBytes())));
	}

	/** A binding of eagerly compiled bytes. */
	public LuaBinding(LuaCompiledSource source) {
		this.managedSource = java.util.Objects.requireNonNull(source, "source");
	}

	/** Binds the immutable source in the owning CamelContext before using its URI. */
	@Override
	public synchronized void bind(CamelContext context) {
		if (managedSource == null) {
			return;
		}
		if (boundContext != null) {
			if (boundContext != context || context.getRegistry().lookupByName(bindingName) != managedSource) {
				throw new IllegalStateException("Lua source binding is not owned by the expected CamelContext");
			}
			bindingOwners++;
			return;
		}
		Object existing = context.getRegistry().lookupByName(bindingName);
		if (existing != null && existing != managedSource) {
			throw new IllegalStateException("Lua source binding already exists: " + bindingName);
		}
		if (existing == null) context.getRegistry().bind(bindingName, managedSource);
		boundContext = context;
		bindingOwners = 1;
	}

	/** Unbinds after all routes using this configuration have been removed. */
	public synchronized void unbind(CamelContext context) {
		if (managedSource == null) {
			return;
		}
		if (boundContext != null && boundContext != context) {
			throw new IllegalArgumentException("Lua source must be unbound by its owning CamelContext");
		}
		if (bindingOwners > 1) {
			bindingOwners--;
			return;
		}
		Object existing = context.getRegistry().lookupByName(bindingName);
		if (existing == managedSource) {
			context.getRegistry().unbind(bindingName);
		}
		if (boundContext == context) boundContext = null;
		bindingOwners = 0;
		managedSource = null;
	}

	@Override
	public synchronized void close() {
		if (boundContext != null) unbind(boundContext);
	}

	public String getContentIdentity() {
		LuaCompiledSource source = managedSource;
		return source == null ? null : source.contentIdentity();
	}

	public synchronized boolean isManagedSourceBound() {
		return managedSource != null && boundContext != null
				&& boundContext.getRegistry().lookupByName(bindingName) == managedSource;
	}

	/** The {@code lua:} endpoint URI of the bound source. */
	public String uri() {
		return "lua:managed:" + bindingName + "?timeoutSeconds=" + getTimeoutSeconds();
	}

	public String getTimeoutSeconds() {
		return timeoutSeconds;
	}

	public void setTimeoutSeconds(String timeoutSeconds) {
		this.timeoutSeconds = timeoutSeconds;
	}
}
