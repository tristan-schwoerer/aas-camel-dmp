/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.aas.http;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.camel.CamelContext;

/**
 * The data plane's one client of the AAS repository: element reads, {@code $value} writes and event enrichment,
 * all with the repository authorization discovery uses (SPEC §11). One per Camel context, bound in its registry.
 */
public final class AasHttp {
	public static final String REGISTRY_NAME = "dmpAasHttp";
	public static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);
	private static final HttpClient CLIENT = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10))
			.followRedirects(HttpClient.Redirect.NEVER).build();

	private volatile String authorization;

	/** The context's client, bound on first use without authorization. */
	public static AasHttp of(CamelContext context) {
		synchronized (context) {
			AasHttp bound = context.getRegistry().lookupByNameAndType(REGISTRY_NAME, AasHttp.class);
			if (bound == null) {
				bound = new AasHttp();
				context.getRegistry().bind(REGISTRY_NAME, AasHttp.class, bound);
			}
			return bound;
		}
	}

	/** The {@code Authorization} header value every request carries, or null for none. */
	public void authorize(String authorization) {
		if (authorization != null && (authorization.isBlank() || authorization.contains("\r") || authorization.contains("\n")))
			throw new IllegalArgumentException("authorization must be a single header value");
		this.authorization = authorization;
	}

	/**
	 * The URL of the element at {@code idShortPath} below a submodel endpoint. The path is one percent-encoded
	 * segment: {@code Items[0]} becomes {@code Items%5B0%5D}, which BaSyx requires (a raw bracket is a 400).
	 */
	public static String elementUrl(String submodelEndpoint, String idShortPath) {
		if (idShortPath == null || idShortPath.isEmpty()) return submodelEndpoint;
		return submodelEndpoint + "/submodel-elements/" + URLEncoder.encode(idShortPath, StandardCharsets.UTF_8).replace("+", "%20");
	}

	/** A GET of JSON, without a thread of its own: the caller's timeout cancels the request. */
	public String get(String url, Duration timeout) throws IOException {
		CompletableFuture<HttpResponse<String>> response = CLIENT.sendAsync(request(url, timeout).GET().build(),
				HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
		try {
			return checked("AAS read", response.get(timeout.toMillis(), TimeUnit.MILLISECONDS));
		} catch (TimeoutException timedOut) {
			throw new IOException("AAS read timed out after " + timeout.toMillis() + " ms", timedOut);
		} catch (InterruptedException interrupted) {
			Thread.currentThread().interrupt();
			throw new IOException("AAS read interrupted", interrupted);
		} catch (ExecutionException failed) {
			throw new IOException("AAS read failed", failed.getCause());
		} finally {
			response.cancel(true);
		}
	}

	/** A PATCH of an element's {@code $value} with its ValueOnly JSON. */
	public void patchValue(String elementUrl, String json) throws IOException {
		HttpRequest request = request(elementUrl + "/$value", REQUEST_TIMEOUT)
				.method("PATCH", HttpRequest.BodyPublishers.ofString(json, StandardCharsets.UTF_8)).build();
		try {
			checked("AAS write", CLIENT.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8)));
		} catch (InterruptedException interrupted) {
			Thread.currentThread().interrupt();
			throw new IOException("AAS write interrupted", interrupted);
		}
	}

	private HttpRequest.Builder request(String url, Duration timeout) {
		HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(url)).timeout(timeout)
				.header("Content-Type", "application/json").header("Accept", "application/json");
		String current = authorization;
		if (current != null) request.header("Authorization", current);
		return request;
	}

	private static String checked(String what, HttpResponse<String> response) throws IOException {
		int status = response.statusCode();
		if (status >= 200 && status < 300) return response.body();
		String detail = response.body() == null ? "" : response.body();
		if (detail.length() > 512) detail = detail.substring(0, 512) + "…";
		throw new IOException(what + " failed with HTTP " + status + (detail.isBlank() ? "" : ": " + detail));
	}
}
