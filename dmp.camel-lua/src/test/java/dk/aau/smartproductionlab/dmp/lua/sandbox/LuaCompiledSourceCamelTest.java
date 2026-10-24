/*******************************************************************************
 * Copyright (C) 2026 the Eclipse BaSyx Authors
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package dk.aau.smartproductionlab.dmp.lua.sandbox;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.test.junit5.CamelTestSupport;
import dk.aau.smartproductionlab.dmp.lua.configuration.LuaBinding;
import org.junit.jupiter.api.Test;

class LuaCompiledSourceCamelTest extends CamelTestSupport {
	@Test
	void keyedInputPreservesExactAuthoredKeysAndPayloadLevels() throws Exception {
		String script = "function aimc_main(sources) return { result = sources } end";
		var configuration = new LuaBinding(LuaCompiledSource.fromBytes(script.getBytes(StandardCharsets.UTF_8), "exact-keys", 256));
		configuration.bind(context);
		try {
			context.addRoutes(new RouteBuilder() {
				@Override public void configure() {
					from("direct:exact-keys").to(configuration.uri());
				}
			});
			context.start();
			for (String key : java.util.List.of("body", "Body", "a,b", " spaced ", "Invoke", "a&unknown=bad,#unicode")) {
				Map<String, Object> input = Map.of(key, Map.of(key, Map.of("value", "payload")));
				Map<?, ?> result = template.requestBody("direct:exact-keys", input, Map.class);
				assertEquals(input, result.get("result"), key);
			}
			Map<String, Object> input = Map.of("body", "literal", "Body", Map.of("body", "nested"));
			assertEquals(input, template.requestBody("direct:exact-keys", input, Map.class).get("result"));
		} finally {
			context.stop();
			configuration.close();
		}
	}

	@Test
	void compiledManagedSourceIsReusedAcrossExchanges() throws Exception {
		AtomicInteger compileCount = new AtomicInteger();
		byte[] bytes = "function aimc_main(sources) return { value = sources.body.value } end"
				.getBytes(StandardCharsets.UTF_8);
		LuaCompiledSource source = LuaCompiledSource.fromBytes(bytes, "mapping:compile-once", 256,
				(script, identity) -> {
					compileCount.incrementAndGet();
					return CompiledLuaScript.compile(script, identity);
				});
		LuaBinding configuration = new LuaBinding(source);
		configuration.bind(context);
		try {
			context.addRoutes(new RouteBuilder() {
				@Override
				public void configure() {
					from("direct:managed").to(configuration.uri());
				}
			});
			context.start();

			for (int i = 0; i < 10; i++) {
				Map<?, ?> result = template.requestBody("direct:managed", Map.of("body", Map.of("value", i)), Map.class);
				assertEquals(i, ((Number) result.get("value")).intValue());
			}
			assertEquals(1, compileCount.get());
		} finally {
			try {
				context.stop();
			} finally {
				configuration.close();
			}
		}
	}
}
