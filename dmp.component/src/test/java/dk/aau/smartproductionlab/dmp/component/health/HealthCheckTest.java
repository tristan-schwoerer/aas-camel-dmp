/*******************************************************************************
 * Copyright (C) 2023 the Eclipse BaSyx Authors
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
package dk.aau.smartproductionlab.dmp.component.health;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.test.junit5.CamelTestSupport;
import dk.aau.smartproductionlab.dmp.component.ManagedRuntimeStatus;
import dk.aau.smartproductionlab.dmp.management.repository.MappingReconciliationService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Health reflects the runtime status: ready only after the initial repository
 * reconciliation and with the control route started, rendered from a fixed
 * safe projection.
 *
 * @author danish
 */
class HealthCheckTest extends CamelTestSupport {

	/** Reserve a free port so the test does not collide with a running DMP on 8080. */
	static {
		try (ServerSocket socket = new ServerSocket(0)) {
			System.setProperty(HealthCheckUtils.HEALTH_PORT_SYSTEM_PROPERTY,
					String.valueOf(socket.getLocalPort()));
		} catch (IOException e) {
			throw new ExceptionInInitializerError(e);
		}
	}

	@Override
	protected RouteBuilder createRouteBuilder() {
		return new HealthCheckRouteBuilder();
	}

	@AfterEach
	void resetStatus() {
		ManagedRuntimeStatus.reset();
	}

	@Test
	void unavailableBeforeTheInitialReconciliation() throws Exception {
		ManagedRuntimeStatus.reset();
		java.net.http.HttpResponse<String> response = health();
		assertEquals(java.net.HttpURLConnection.HTTP_UNAVAILABLE, response.statusCode());
		JsonObject value = JsonParser.parseString(response.body()).getAsJsonObject();
		assertEquals("starting", value.get("state").getAsString());
		assertFalse(value.get("ready").getAsBoolean());
	}

	@Test
	void requestsStayOpenForTheLongestInvocationTimeout() throws Exception {
		health();
		long timeout = context.getComponent("jetty", org.apache.camel.component.jetty12.JettyHttpComponent12.class)
				.getContinuationTimeout();
		assertEquals(HealthCheckRouteBuilder.CONTINUATION_TIMEOUT_MS, timeout, "not Jetty's 30 s");
		assertTrue(timeout > 15 * 60 * 1000L, "beyond PT15M");
	}

	@Test
	void okOnceTheInitialScanCompletedAndTheControlRouteStarted() throws Exception {
		ManagedRuntimeStatus.beginManaged();
		ManagedRuntimeStatus.controlRouteStarted();
		ManagedRuntimeStatus.publish(new MappingReconciliationService.Report(true, false, 0, 0, 0, List.of(), List.of()));
		java.net.http.HttpResponse<String> response = health();
		assertEquals(java.net.HttpURLConnection.HTTP_OK, response.statusCode());
		JsonObject value = JsonParser.parseString(response.body()).getAsJsonObject();
		assertEquals("ready", value.get("state").getAsString());
		assertTrue(value.get("initialScanComplete").getAsBoolean());
	}

	@Test
	void healthUsesTheCachedSafeProjectionRatherThanNativeDetailObjects() throws Exception {
		ManagedRuntimeStatus.beginManaged();
		java.net.http.HttpResponse<String> response = health();
		assertEquals(java.net.HttpURLConnection.HTTP_UNAVAILABLE, response.statusCode());
		String body = response.body();
		JsonObject value = JsonParser.parseString(body).getAsJsonObject();
		assertEquals(java.util.Set.of("ready", "state", "initialScanComplete", "category"), value.keySet());
		assertFalse(body.contains("route.id"));
		assertFalse(body.contains("exception"));
	}

	private static java.net.http.HttpResponse<String> health() throws IOException {
		try {
			return java.net.http.HttpClient.newHttpClient().send(java.net.http.HttpRequest.newBuilder(
					java.net.URI.create(HealthCheckUtils.getHealthCheckEndpointRequestURI())).GET().build(),
					java.net.http.HttpResponse.BodyHandlers.ofString());
		} catch (InterruptedException interrupted) {
			Thread.currentThread().interrupt();
			throw new IOException(interrupted);
		}
	}
}
