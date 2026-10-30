/*******************************************************************************
 * Copyright (C) 2026 the aas-camel-dmp Authors
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
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package dk.aau.smartproductionlab.dmp.aas.operation;


import java.util.Map;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import java.net.HttpURLConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Final step of the declarative operation-delegation route: after the
 * response-direction Lua transform, take the value of the ResponseSink that
 * selects the Operation and answer with the output and InOut variables as the
 * {@code OperationVariable[]} BaSyx delegation expects, built from the DECLARED
 * reply types (see {@link OperationPayloads#toOperationVariables}).
 *
 * @author aas-camel-dmp
 */
public class OperationReplyProcessor implements Processor {

	private static final Logger LOGGER = LoggerFactory.getLogger(OperationReplyProcessor.class);

	private final OperationContext context;

	public OperationReplyProcessor(OperationContext context) {
		this.context = context;
	}

	@Override
	public void process(Exchange exchange) {
		Map<String, Object> replyMap = callerOutputs(exchange.getMessage().getBody());
		exchange.getMessage().setBody(
				OperationPayloads.toOperationVariables(coerce(replyMap), context.callerResponseTypes,
						context.callerOutputOrder, context.callerOutputTemplates));
		LOGGER.debug("Operation delegation reply prepared");
		exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, HttpURLConnection.HTTP_OK);
	}

	/**
	 * Fails when the response Lua's result does not answer the caller: run before the
	 * response writes, so an invalid reply writes nothing back to the AAS.
	 */
	public void validate(Object result) {
		callerOutputs(result);
	}

	/**
	 * The caller's outputs, from the response Lua's values for the ResponseSinks that
	 * select the Operation: either one sink for the whole Operation, whose value is an
	 * object keyed by output and InOut idShort, or one sink per variable, placed at its
	 * path. Every variable not marked Optional must be answered; a per-variable sink may
	 * be omitted when its variable is Optional. Without such sinks the Operation answers
	 * with no outputs; the device reply is never matched by name.
	 */
	private Map<String, Object> callerOutputs(Object result) {
		if (!context.explicitResponseMapping) return new LinkedHashMap<>();
		Object outputs;
		String origin;
		if (!context.callerReplyBindings.isEmpty()) {
			Map<String, Object> values = OperationPayloads.keyedValues(result, "response Lua");
			List<String> present = context.callerReplyBindings.keySet().stream().filter(values::containsKey)
					.sorted().toList();
			outputs = present.isEmpty() ? new LinkedHashMap<>()
					: dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline.PayloadAssembler
							.assemble(values, present, context.callerReplyBindings);
			origin = "the Operation variable SinkIds " + String.join(", ", present);
		} else if (context.callerReplySinkId != null) {
			outputs = OperationPayloads.requireSinkValue(result, context.callerReplySinkId);
			origin = "the Operation SinkId " + context.callerReplySinkId;
		} else {
			return new LinkedHashMap<>();
		}
		if (!(outputs instanceof Map<?, ?> map)) {
			throw new OperationValidationException("DMP_RESPONSE_MAPPING_FAILED", 500,
					"Response Lua must give " + origin + " an object keyed by output variable idShort",
					java.util.List.of());
		}
		Map<String, Object> named = new LinkedHashMap<>();
		map.forEach((key, value) -> named.put(String.valueOf(key), value));
		Map<String, Object> selected = OperationPayloads.selectCallerOutputs(named, context.callerOutputOrder);
		// Every output and InOut variable (and collection member) not marked Optional must be answered.
		context.callerOutputTemplates.forEach((name, template) -> OptionalQualifier.requirePresent(template,
				selected.containsKey(name), selected.get(name), name, path -> new OperationValidationException(
						"DMP_RESPONSE_MAPPING_FAILED", 500, "Response Lua omitted the required output " + path,
						java.util.List.of())));
		return selected;
	}

	/**
	 * Applies the DECLARED types to the reply before it goes back to the caller,
	 * mirroring the request leg exactly: a declared {@code xs:string} field must
	 * come back as a string even when it looks numeric, and the correlation
	 * Uuid must be stringified for the same reason.
	 *
	 * <p>Without this the two legs disagreed: a caller passing a bare {@code 1}
	 * was published to the device as {@code "1"} (string), but the device's
	 * echoed {@code "1"} came back to that caller as {@code 1.0}.
	 */
	private Map<String, Object> coerce(Map<String, Object> replyMap) {
		if (replyMap == null) {
			return null;
		}
		OperationPayloads.coerceFields(replyMap, context.callerResponseTypes);
		context.callerOutputTemplates.forEach((name, template) -> {
			if (replyMap.containsKey(name)) replyMap.put(name, OperationVariableValues.coerce(template, replyMap.get(name)));
		});
		return replyMap;
	}
}
