package dk.aau.smartproductionlab.dmp.opcua.canonical;

import org.apache.camel.Processor;
import dk.aau.smartproductionlab.dmp.core.plan.Source;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;

/**
 * Normalizes Milo monitored DataValues before they enter the shared source cache: a sample
 * contributes its value only, as an MQTT or HTTP source does (SPEC §5.2), so the AID schema
 * describes what Lua receives. A sample without a Good status contributes nothing.
 */
public final class OpcUaSourceEventAdapter {
	public Processor normalizer(Source source) {
		return exchange -> {
			Object body = exchange.getMessage().getBody();
			// An event notification is already its selected fields as JSON values (EventSelection).
			if (body instanceof java.util.Map<?, ?>) {
				exchange.setProperty(Source.NORMALIZED_VALUE_PROPERTY, true);
				exchange.setProperty(Source.CONTRIBUTION_PROPERTY, true);
				return;
			}
			if (!(body instanceof DataValue dataValue)
					|| dataValue.getStatusCode() == null || !dataValue.getStatusCode().isGood()
					|| dataValue.getValue() == null) {
				exchange.setProperty(Source.CONTRIBUTION_PROPERTY, false);
				return;
			}
			Object value = OpcUaCanonical.jsonValue(dataValue.getValue());
			exchange.getMessage().setBody(value == null ? dk.aau.smartproductionlab.dmp.core.util.JsonPointer.JSON_NULL : value);
			exchange.setProperty(Source.NORMALIZED_VALUE_PROPERTY, true);
			exchange.setProperty(Source.CONTRIBUTION_PROPERTY, true);
		};
	}
}
