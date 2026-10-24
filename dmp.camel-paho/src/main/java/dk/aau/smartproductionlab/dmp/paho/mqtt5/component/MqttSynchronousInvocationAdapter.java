package dk.aau.smartproductionlab.dmp.paho.mqtt5.component;

import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.ExchangePattern;
import org.apache.camel.model.ProcessorDefinition;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.ConnectorInvocationException;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.SynchronousInvocationAdapter;
import dk.aau.smartproductionlab.dmp.core.util.JsonCodec;

/**
 * MQTT 5 invocation binding on {@code dmp-paho-mqtt5:} ({@code MqttAction}): request/reply with
 * reply normalization when the endpoint declares a reply topic, or a one-way invoke-only publish.
 */
public final class MqttSynchronousInvocationAdapter implements SynchronousInvocationAdapter {
	private static final String SCHEME = DmpPahoMqtt5Component.SCHEME + ":";

	@Override
	public boolean supports(String endpointUri) {
		return endpointUri != null && endpointUri.startsWith(SCHEME);
	}

	@Override
	public java.util.Set<String> ownedEndpointUris(String endpointUri, InvocationSchema schema) {
		if (!invokeOnly(schema)) requireReplyTopic(endpointUri);
		return java.util.Set.of(endpointUri);
	}

	@Override
	public void configureInvoke(ProcessorDefinition<?> route, String endpointUri, InvocationSchema schema) {
		if (invokeOnly(schema)) {
			// No reply topic: the command is published one way; the broker's acknowledgement
			// (QoS 1/2) ends the call, and the action has no output.
			route.doTry()
					.to(ExchangePattern.InOnly, endpointUri)
					.process(exchange -> exchange.getMessage().setBody(new java.util.LinkedHashMap<String, Object>()))
				.doCatch(Exception.class)
					.process(exchange -> {
						throw translateFailure(exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Throwable.class));
					})
				.end();
			return;
		}
		requireReplyTopic(endpointUri);
		route.doTry()
				.to(ExchangePattern.InOut, endpointUri)
				.process(exchange -> exchange.getMessage().setBody(reply(exchange.getMessage().getBody())))
			.doCatch(Exception.class)
				.process(exchange -> {
					throw translateFailure(exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Throwable.class));
				})
			.end();
	}

	/** The correlated reply: a JSON object, or nothing. */
	static Map<String, Object> reply(Object body) {
		if (body instanceof Map<?, ?> map) return stringMap(map);
		if (body == null) return Map.of();
		Object decoded = body instanceof String text ? JsonCodec.parseJson(text)
				: body instanceof byte[] bytes ? JsonCodec.parseJson(new String(bytes, java.nio.charset.StandardCharsets.UTF_8)) : body;
		if (!(decoded instanceof Map<?, ?> map)) {
			throw new ConnectorInvocationException(ConnectorInvocationException.Category.REMOTE,
					"MQTT_REPLY_INVALID", "MQTT response is not a JSON object", Map.of());
		}
		return stringMap(map);
	}

	/** An action the compiler bound without a reply topic (requestReply=false). */
	private static boolean invokeOnly(InvocationSchema schema) {
		return Boolean.FALSE.equals(schema.connectorOptions().get("requestReply"));
	}

	/** A request/reply needs the reply topic its endpoint subscribes. */
	private static void requireReplyTopic(String endpointUri) {
		int query = endpointUri.indexOf('?');
		if (query < 0 || java.util.Arrays.stream(endpointUri.substring(query + 1).split("&"))
				.noneMatch(option -> option.startsWith("replyTopic=") && option.length() > "replyTopic=".length())) {
			throw new ConnectorInvocationException(ConnectorInvocationException.Category.CONFIGURATION,
					"MQTT_REQUEST_REPLY_NOT_DECLARED",
					"MQTT action form must declare request/reply and a response topic", Map.of());
		}
	}

	static ConnectorInvocationException connectorFailure(Throwable failure) {
		for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
			if (cause instanceof ConnectorInvocationException typed) return typed;
			if (cause.getCause() == cause) break;
		}
		return null;
	}

	static ConnectorInvocationException translateFailure(Throwable failure) {
		if (failure == null) failure = new IllegalStateException("MQTT request/reply failed without a cause");
		ConnectorInvocationException typed = connectorFailure(failure);
		if (typed != null) return typed;
		if (hasTimeoutCause(failure)) {
			return new ConnectorInvocationException(ConnectorInvocationException.Category.TIMEOUT,
					"MQTT_REQUEST_TIMEOUT", "Timed out awaiting correlated MQTT reply",
					Map.of());
		}
		return new ConnectorInvocationException(ConnectorInvocationException.Category.REMOTE,
				"MQTT_REQUEST_FAILED", "MQTT request/reply exchange failed",
				Map.of());
	}

	private static boolean hasTimeoutCause(Throwable failure) {
		for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
			if (cause instanceof java.util.concurrent.TimeoutException
					|| cause instanceof org.apache.camel.ExchangeTimedOutException) return true;
			if (cause.getCause() == cause) break;
		}
		return false;
	}

	private static Map<String, Object> stringMap(Map<?, ?> value) {
		Map<String, Object> result = new java.util.LinkedHashMap<>();
		value.forEach((key, item) -> result.put(String.valueOf(key), item));
		return result;
	}
}
