/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.lua.configuration;

import java.util.Objects;

import dk.aau.smartproductionlab.dmp.core.plan.BundleResources;
import dk.aau.smartproductionlab.dmp.core.plan.Endpoint;
import dk.aau.smartproductionlab.dmp.lua.sandbox.LuaCompiledSource;

/**
 * A Lua transformation: a compiled source, an immutable prototype several bundles may share. Each build binds it
 * under a name of its own ({@link LuaBinding}).
 */
public record LuaScript(LuaCompiledSource source) implements Endpoint {
	public LuaScript {
		Objects.requireNonNull(source, "source");
	}

	@Override
	public String uri(BundleResources resources) {
		return resources.own(new LuaBinding(source)).uri();
	}
}
