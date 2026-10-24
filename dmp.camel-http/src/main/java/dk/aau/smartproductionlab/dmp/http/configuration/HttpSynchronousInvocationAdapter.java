package dk.aau.smartproductionlab.dmp.http.configuration;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.model.ProcessorDefinition;
import static org.apache.camel.builder.Builder.constant;
import org.apache.camel.component.http.HttpClientConfigurer;
import org.apache.camel.http.base.HttpOperationFailedException;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.ConnectorInvocationException;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.SynchronousInvocationAdapter;
import dk.aau.smartproductionlab.dmp.core.util.JsonCodec;

/** HTTP InOut binding for a WoT form's declared methodName and JSON payload. */
public final class HttpSynchronousInvocationAdapter implements SynchronousInvocationAdapter {

	@Override
	public boolean supports(String endpointUri) {
		return endpointUri != null && (endpointUri.startsWith("http://")
				|| endpointUri.startsWith("https://"));
	}

	@Override
	public java.util.Set<String> ownedEndpointUris(String endpointUri, InvocationSchema schema) {
		return java.util.Set.of(invocationUri(endpointUri, timeoutMillis(schema)));
	}

	@Override
	public void configureInvoke(ProcessorDefinition<?> route, String endpointUri, InvocationSchema schema) {
		Object method = schema.connectorOptions().get("httpMethod");
		long timeoutMillis = timeoutMillis(schema);
		route.setHeader(Exchange.HTTP_METHOD, constant(method == null ? "POST" : String.valueOf(method)))
				.setHeader(Exchange.CONTENT_TYPE, constant("application/json"))
				.setHeader("Accept", constant("application/json"))
				.process(exchange -> exchange.getMessage().setBody(JsonCodec.toJson(exchange.getMessage().getBody())))
				.doTry()
					.to(invocationUri(endpointUri, timeoutMillis))
					.process(exchange -> exchange.getMessage().setBody(reply(exchange.getContext(), exchange.getMessage().getBody())))
				.doCatch(Exception.class)
					.process(exchange -> {
						throw failure(exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class), timeoutMillis);
					})
				.end();
	}

	/** The device's reply: a JSON object, or nothing. */
	static Map<String, Object> reply(CamelContext context, Object reply) {
		if (reply instanceof Map<?, ?> map) return stringMap(map);
		String text = reply == null ? null : context.getTypeConverter().convertTo(String.class, reply);
		if (text == null || text.isBlank()) return Map.of();
		Object decoded = JsonCodec.parseJson(text);
		if (!(decoded instanceof Map<?, ?> resultMap)) {
			throw new ConnectorInvocationException(ConnectorInvocationException.Category.REMOTE,
					"HTTP_REPLY_INVALID", "HTTP action reply is not a JSON object", Map.of());
		}
		return stringMap(resultMap);
	}

	static ConnectorInvocationException failure(Exception error, long timeoutMillis) {
		for (Throwable current = error; current != null; current = current.getCause()) {
			if (current instanceof ConnectorInvocationException typed) return typed;
			if (current instanceof java.net.SocketTimeoutException
					|| current instanceof org.apache.hc.core5.util.TimeoutValueException)
				return new ConnectorInvocationException(ConnectorInvocationException.Category.TIMEOUT,
						"HTTP_INVOKE_TIMEOUT", "HTTP action endpoint timed out", Map.of("timeoutSeconds", timeoutMillis / 1000));
		}
		HttpOperationFailedException failedResponse = findHttpFailure(error);
		if (failedResponse != null) {
			Map<String, Object> details = new LinkedHashMap<>();
			details.put("status", failedResponse.getStatusCode());
			details.put("statusText", String.valueOf(failedResponse.getStatusText()));
			Object deviceError = safeErrorBody(failedResponse.getResponseBody());
			if (deviceError != null) details.put("deviceError", deviceError);
			return new ConnectorInvocationException(ConnectorInvocationException.Category.REMOTE,
					"HTTP_" + failedResponse.getStatusCode(), "HTTP action endpoint returned an error", details);
		}
		return new ConnectorInvocationException(ConnectorInvocationException.Category.TRANSPORT,
				"HTTP_INVOKE_FAILED", "HTTP action invocation failed: " + causalMessage(error),
				Map.of("cause", causalMessage(error)));
	}

	public static final class NoRetryHttpClientConfigurer implements HttpClientConfigurer {
		@Override
		public void configureHttpClient(HttpClientBuilder clientBuilder) {
			clientBuilder.disableAutomaticRetries();
		}
	}

	/**
	 * The action's endpoint: the form's client options ({@link HttpClientOptions}: no retries, its headers) with
	 * the action's timeout in place of the default one. A bare URL gets a client that never retries.
	 */
	static String invocationUri(String endpointUri, long timeoutMillis) {
		if (endpointUri.contains("httpClientConfigurer="))
			return endpointUri.replaceAll("([?&])(connectTimeout|responseTimeout)=\\d+", "$1$2=" + timeoutMillis);
		return endpointUri + (endpointUri.contains("?") ? "&" : "?") + "httpClientConfigurer=#class:"
				+ NoRetryHttpClientConfigurer.class.getName() + "&connectTimeout=" + timeoutMillis + "&responseTimeout=" + timeoutMillis;
	}

	private static long timeoutMillis(InvocationSchema schema) {
		return number(schema.connectorOptions().get("timeoutSeconds"), 30) * 1000;
	}

	private static long number(Object value, long fallback) {
		if (value instanceof Number number) return number.longValue();
		try { return value == null ? fallback : Long.parseLong(String.valueOf(value)); }
		catch (NumberFormatException ignored) { return fallback; }
	}

	private static String causalMessage(Throwable error) {
		StringBuilder text = new StringBuilder();
		for (Throwable current = error; current != null; current = current.getCause()) {
			if (!text.isEmpty()) text.append(" <- ");
			text.append(current.getClass().getSimpleName()).append(": ").append(current.getMessage());
		}
		return text.toString();
	}

	private static HttpOperationFailedException findHttpFailure(Throwable error) {
		for (Throwable current = error; current != null; current = current.getCause()) {
			if (current instanceof HttpOperationFailedException failure) return failure;
		}
		return null;
	}

	private static Object safeErrorBody(String body) {
		if (body == null || body.isBlank()) return null;
		try {
			Object decoded = JsonCodec.parseJson(body);
			if (decoded instanceof Map<?, ?> map) {
				Map<String, Object> safe = new LinkedHashMap<>();
				for (String key : java.util.List.of("code", "error", "message")) {
					if (map.containsKey(key)) safe.put(key, map.get(key));
				}
				return safe.isEmpty() ? null : safe;
			}
		} catch (RuntimeException ignored) {
			// Do not echo arbitrary device response bodies to the caller.
		}
		return null;
	}

	private static Map<String, Object> stringMap(Map<?, ?> map) {
		Map<String, Object> result = new LinkedHashMap<>();
		map.forEach((key, value) -> result.put(String.valueOf(key), value));
		return result;
	}
}
