package dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import dk.aau.smartproductionlab.dmp.core.plan.Response.ResponseSource;
import dk.aau.smartproductionlab.dmp.core.util.JsonPointer;

/** Projects a validated device reply into the authored response SourceIds. */
public final class ResponseProjectionProcessor implements Processor {
	private final List<ResponseSource> bindings;

	public ResponseProjectionProcessor(List<ResponseSource> bindings) {
		this.bindings = List.copyOf(bindings);
	}

	@Override
	public void process(Exchange exchange) {
		exchange.getMessage().setBody(project(exchange.getMessage().getBody(), bindings));
	}

	public static Map<String, Object> project(Object parsed, List<ResponseSource> bindings) {
		Map<String, Object> sources = new LinkedHashMap<>();
		for (ResponseSource binding : bindings) {
			String sourceId = binding.sourceId();
			if (sources.containsKey(sourceId)) {
				throw projectionError("duplicate ResponseSource id " + sourceId);
			}
			Object value = parsed == JsonPointer.JSON_NULL ? null : parsed;
			String path = binding.valuePath();
			if (path != null && !path.isBlank()) {
				Object selected = JsonPointer.extract(parsed, path)
						.orElseThrow(() -> projectionError("missing ResponseSource valuePath " + path
								+ " for " + sourceId));
				value = selected == JsonPointer.JSON_NULL ? null : selected;
			}
			sources.put(sourceId, value);
		}
		return sources;
	}

	private static PayloadValidationException projectionError(String message) {
		return new PayloadValidationException(PayloadValidationException.Stage.MAPPING,
				message, List.of());
	}
}
