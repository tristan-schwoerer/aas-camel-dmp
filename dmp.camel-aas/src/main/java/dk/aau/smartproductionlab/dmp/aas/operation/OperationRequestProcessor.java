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

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import dk.aau.smartproductionlab.dmp.core.util.JsonCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * First step of the declarative operation-delegation route: BaSyx (Java and Go alike) POSTs the input and InOut
 * variables as an {@code OperationVariable[]} (JSON array); it is flattened into the {@code {idShort: value}} map
 * the mapping Lua expects, keyed by the authored SourceIds. Any other body is the caller's 400.
 *
 * @author aas-camel-dmp
 */
public class OperationRequestProcessor implements Processor {

	private static final Logger LOGGER = LoggerFactory.getLogger(OperationRequestProcessor.class);
	private final java.util.Map<String, org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElement> inputTemplates;
	private final java.util.Map<String, String> inputSourcePaths;

	/**
	 * @param inputSourcePaths
	 *            each Source on the Operation: its SourceId and the path it selects ({@code $} for the whole
	 *            request)
	 */
	public OperationRequestProcessor(
			java.util.Map<String, org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElement> inputTemplates,
			java.util.Map<String, String> inputSourcePaths) {
		if (inputSourcePaths == null || inputSourcePaths.isEmpty())
			throw new IllegalArgumentException("an Operation request needs at least one Source on the Operation");
		this.inputTemplates = inputTemplates == null ? java.util.Map.of() : inputTemplates;
		this.inputSourcePaths = inputSourcePaths;
	}

	@Override
	public void process(Exchange exchange) throws Exception {
		Object body = decodeRestJson(exchange.getMessage().getBody());
		if (!(body instanceof java.util.List<?>) || !OperationPayloads.isOperationVariables(body)) {
			throw new OperationValidationException("DMP_OPERATION_VALIDATION_FAILED", 400,
					"Operation request must be the OperationVariable array BaSyx delegates", java.util.List.of());
		}
		LOGGER.debug("flattening the delegated OperationVariable[]");
		body = OperationPayloads.flattenOperationVariables(body, inputTemplates);
		requireInputs(body);
		exchange.getMessage().setBody(JsonCodec.copyValue(keyedInputs(body)));
	}

	/**
	 * The request keyed by SourceId: a source on the whole Operation ({@code $}) gets the
	 * request as it is, a source on a variable gets the value at its path. A variable
	 * marked Optional that the caller left out gives its source no value.
	 */
	private java.util.Map<String, Object> keyedInputs(Object body) {
		java.util.Map<String, Object> keyed = new java.util.LinkedHashMap<>();
		inputSourcePaths.forEach((sourceId, path) -> {
			if ("$".equals(path)) {
				keyed.put(sourceId, body);
				return;
			}
			if (!(body instanceof java.util.Map<?, ?> values)) {
				throw new OperationValidationException("DMP_OPERATION_VALIDATION_FAILED", 400,
						"Operation request must be an object keyed by input variable idShort", java.util.List.of());
			}
			java.util.Optional<Object> selected = dk.aau.smartproductionlab.dmp.core.util.JsonPointer
					.extract(values, path);
			if (selected.isEmpty()) {
				if (optionalVariable(path)) return;
				throw new OperationValidationException("DMP_OPERATION_VALIDATION_FAILED", 400,
						"Operation request omits a referenced input value", java.util.List.of(sourceId));
			}
			Object value = selected.get();
			keyed.put(sourceId, value == dk.aau.smartproductionlab.dmp.core.util.JsonPointer.JSON_NULL ? null : value);
		});
		return keyed;
	}

	/** Whether the input variable at the root of {@code path} ({@code $.Var...}) is marked Optional. */
	private boolean optionalVariable(String path) {
		java.util.List<String> tokens = dk.aau.smartproductionlab.dmp.core.util.JsonPointer.parse(path).tokens();
		if (tokens.isEmpty()) return false;
		org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElement template = inputTemplates.get(tokens.get(0));
		return template != null && OptionalQualifier.optional(template);
	}

	/**
	 * The caller must give every input and InOut variable (and collection member) not marked
	 * Optional; a missing one fails before the request Lua runs and the device is called.
	 */
	private void requireInputs(Object body) {
		if (inputTemplates.isEmpty()) return;
		if (!(body instanceof java.util.Map<?, ?> values)) {
			throw new OperationValidationException("DMP_OPERATION_VALIDATION_FAILED", 400,
					"Operation request must be an object keyed by input variable idShort", java.util.List.of());
		}
		inputTemplates.forEach((name, template) -> OptionalQualifier.requirePresent(template,
				values.containsKey(name), values.get(name), name, path -> new OperationValidationException(
						"DMP_OPERATION_VALIDATION_FAILED", 400, "Operation request omits the required input " + path,
						java.util.List.of(path))));
	}

	private static Object decodeRestJson(Object body) throws java.io.IOException {
		if (body instanceof String text) return JsonCodec.parseJson(text);
		if (body instanceof byte[] bytes) {
			return JsonCodec.parseJson(new String(bytes, java.nio.charset.StandardCharsets.UTF_8));
		}
		if (body instanceof java.io.InputStream stream) {
			return JsonCodec.parseJson(new String(stream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8));
		}
		return body;
	}
}
