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
package dk.aau.smartproductionlab.dmp.lua.component;

import org.apache.camel.Category;
import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.spi.Metadata;
import org.apache.camel.spi.UriEndpoint;
import org.apache.camel.spi.UriParam;
import org.apache.camel.spi.UriPath;
import org.apache.camel.support.DefaultEndpoint;
import dk.aau.smartproductionlab.dmp.core.routebuilder.ManagedRouteResource.AccountedEndpoint;
import dk.aau.smartproductionlab.dmp.core.routebuilder.ManagedRouteResource.AccountingEntry;
import dk.aau.smartproductionlab.dmp.lua.sandbox.LuaCompiledSource;

/**
 * Endpoint for the {@code lua:} scheme (producer-only transformation).
 *
 * <p>
 * URI syntax: {@code lua:managed:<binding>?timeoutSeconds=2},
 * where {@code <binding>} names a {@link LuaCompiledSource} bound in the Camel
 * registry by its {@code LuaBinding}. Lua is never read from files.
 *
 * @author aas-camel-dmp
 */
@UriEndpoint(firstVersion = "1.0.0-SNAPSHOT", scheme = "lua", title = "Lua",
		syntax = "lua:resourcePath", producerOnly = true, category = { Category.SCRIPT })
public class LuaEndpoint extends DefaultEndpoint implements AccountedEndpoint {

	@UriPath(description = "Registry binding of the compiled Lua source, as managed:<binding>")
	@Metadata(required = true)
	private String resourcePath;

	@UriParam(defaultValue = "2", description = "CPU budget per invocation in seconds")
	private double timeoutSeconds = 2;

	private volatile LuaCompiledSource source;

	@Override public java.util.List<AccountingEntry> accounting() {
		LuaCompiledSource source = this.source;
		return source == null ? java.util.List.of() : java.util.List.of(new AccountingEntry(source,
				java.util.Map.of("luaSourceRetainedBytes", source.retainedSourceBytes())));
	}

	public LuaEndpoint() {
	}

	public LuaEndpoint(String endpointUri, LuaComponent component) {
		super(endpointUri, component);
	}

	@Override
	public Producer createProducer() throws Exception {
		if (getResourcePath() == null || !getResourcePath().startsWith("managed:")) {
			throw new IllegalArgumentException("Lua endpoints reference a compiled source as lua:managed:<binding>");
		}
		String name = getResourcePath().substring("managed:".length());
		Object binding = getCamelContext().getRegistry().lookupByName(name);
		if (!(binding instanceof LuaCompiledSource compiled)) {
			throw new IllegalArgumentException("No Lua source bound as '" + name + "'");
		}
		source = compiled;
		return new LuaProducer(this);
	}

	@Override
	public Consumer createConsumer(Processor processor) throws Exception {
		throw new UnsupportedOperationException("The lua component is producer-only (transformation step).");
	}

	/** Human-readable identity of the bound source for diagnostics. */
	public String getDiagnosticIdentity() {
		LuaCompiledSource bound = source;
		return bound == null ? getResourcePath() : bound.diagnosticIdentity();
	}

	public LuaCompiledSource getSource() {
		return source;
	}

	public String getResourcePath() {
		return resourcePath;
	}

	public void setResourcePath(String resourcePath) {
		this.resourcePath = resourcePath;
	}

	public double getTimeoutSeconds() {
		return timeoutSeconds;
	}

	public void setTimeoutSeconds(double timeoutSeconds) {
		this.timeoutSeconds = timeoutSeconds;
	}

}
