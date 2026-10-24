package dk.aau.smartproductionlab.dmp.paho.consumer;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import dk.aau.smartproductionlab.dmp.core.configuration.entity.DataSourceConfiguration;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.SourceEventAdapter;
import dk.aau.smartproductionlab.dmp.core.util.JsonCodec;
import dk.aau.smartproductionlab.dmp.core.util.JsonPointer;

/** Decodes MQTT payload bytes only when the transport/schema declares JSON. */
public final class MqttSourceEventAdapter implements SourceEventAdapter {
	@Override
	public String sourceKind() { return "mqtt"; }

	@Override
	public Processor normalizer(DataSourceConfiguration configuration) {
		return exchange -> {
			Object body = exchange.getMessage().getBody();
			boolean json = jsonContent(exchange) || jsonSchema(configuration.getSchema());
			Object normalized = decode(body, json, textSchema(configuration.getSchema()));
			exchange.getMessage().setBody(normalized);
			exchange.setProperty(NORMALIZED_VALUE_PROPERTY, true);
			exchange.setProperty(CONTRIBUTION_PROPERTY, normalized != null);
		};
	}

	private static Object decode(Object body, boolean declaredJson, boolean bareTextAllowed) throws Exception {
		if (body instanceof Map<?, ?> || body instanceof List<?> || body == JsonPointer.JSON_NULL) {
			return JsonCodec.copyValue(body);
		}
		String text;
		if (body instanceof byte[] bytes) {
			if (!declaredJson) return bytes.clone();
			text = new String(bytes, StandardCharsets.UTF_8);
		} else if (body instanceof InputStream stream) {
			byte[] bytes = stream.readAllBytes();
			if (!declaredJson) return bytes;
			text = new String(bytes, StandardCharsets.UTF_8);
		} else if (body instanceof String value) {
			if (!declaredJson) return value;
			text = value;
		} else {
			return JsonCodec.copyValue(body);
		}
		if (bareTextAllowed) {
			// A string is a JSON string ("READY"), or the bare text a device published (RUNNING, {"opaque":true}).
			try {
				return JsonCodec.parseJson(text) instanceof String value ? value : text;
			} catch (RuntimeException notJson) {
				return text;
			}
		}
		Object decoded = JsonCodec.parseJson(text);
		return decoded == null ? JsonPointer.JSON_NULL : decoded;
	}

	private static boolean jsonContent(Exchange exchange) {
		String contentType = exchange.getMessage().getHeader(Exchange.CONTENT_TYPE, String.class);
		if (contentType == null) return false;
		String mediaType = contentType.split(";", 2)[0].trim().toLowerCase(java.util.Locale.ROOT);
		return "application/json".equals(mediaType) || mediaType.endsWith("+json");
	}

	/**
	 * A payload with a declared data schema is JSON, the WoT MQTT binding's default content type, whatever the
	 * schema's type: a number arrives as {@code 42}, not as bytes. A string schema also takes bare text.
	 */
	private static boolean jsonSchema(Object rawSchema) {
		if (!(rawSchema instanceof Map<?, ?> schema)) return false;
		return schema.containsKey("type") || schema.containsKey("properties") || schema.containsKey("items")
				|| schema.containsKey("const") || schema.containsKey("enum") || schema.containsKey("oneOf");
	}

	private static boolean textSchema(Object rawSchema) {
		return rawSchema instanceof Map<?, ?> schema && "string".equals(schema.get("type"));
	}
}
