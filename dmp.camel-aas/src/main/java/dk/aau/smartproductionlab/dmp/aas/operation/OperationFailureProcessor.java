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
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.ConnectorInvocationException;
import dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline.PayloadValidationException;
import java.net.HttpURLConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import dk.aau.smartproductionlab.dmp.core.util.JsonCodec;

/**
 * Exception mapping of the declarative operation-delegation route
 * ({@code doCatch}): marks the delegation failed (the writeback must NOT
 * store an error payload as the effective value) and answers with an HTTP
 * error status and the AAS API's {@code Result}: one error message with the
 * DMP's code, a safe text and the invocation id as its correlationId. Typed
 * failures map to 400/500/502/504; untyped failures use the status wired
 * by the route creator. Exception messages and connector/validation details
 * are never public diagnostics or log fields; only the names of omitted inputs
 * reach a 400. BaSyx reports a failed delegation by status alone.
 *
 * @author aas-camel-dmp
 */
public class OperationFailureProcessor implements Processor {

	private static final Logger LOGGER = LoggerFactory.getLogger(OperationFailureProcessor.class);

	private final int status;

	public OperationFailureProcessor(OperationContext context, int status) {
		this.status = status;
	}

	@Override
	public void process(Exchange exchange) {
		Throwable caught = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class);
		var seen = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<Throwable, Boolean>());
		for (Throwable cause = caught; cause != null && seen.add(cause); cause = cause.getCause()) {
			if (cause instanceof OperationValidationException || cause instanceof PayloadValidationException
					|| cause instanceof ConnectorInvocationException) {
				caught = cause;
				break;
			}
		}
		int callerStatus = status;
		String code = status == HttpURLConnection.HTTP_GATEWAY_TIMEOUT ? "DMP_DEVICE_TIMEOUT" : "DMP_OPERATION_FAILED";
		String message = status == HttpURLConnection.HTTP_GATEWAY_TIMEOUT ? "Device invocation timed out" : "Operation failed";
		exchange.setProperty(OperationPayloads.OPERATION_DELEGATION_FAILED, true);
		// Typed failures still contain untrusted URI, device and validation text.
		// Only these fixed categories and messages may cross the public boundary.
		if (caught instanceof OperationValidationException validation) {
			// A request the caller got wrong (a missing required input) is the caller's 400.
			callerStatus = validation.getStatus() == HttpURLConnection.HTTP_BAD_REQUEST
					? HttpURLConnection.HTTP_BAD_REQUEST : HttpURLConnection.HTTP_INTERNAL_ERROR;
			code = "DMP_OPERATION_VALIDATION_FAILED";
			message = "Operation validation failed";
			if ("DMP_COMMAND_VALIDATION_FAILED".equals(validation.getCode())) {
				code = "DMP_COMMAND_VALIDATION_FAILED";
				message = "Mapped command violates the declared input schema";
			} else if ("DMP_RESPONSE_VALIDATION_FAILED".equals(validation.getCode())) {
				callerStatus = HttpURLConnection.HTTP_BAD_GATEWAY;
				code = "DMP_RESPONSE_VALIDATION_FAILED";
				message = "Device response violates the declared output schema";
			} else if ("DMP_RESPONSE_MAPPING_FAILED".equals(validation.getCode())) {
				code = "DMP_RESPONSE_MAPPING_FAILED";
				message = "Response mapping failed";
			}
		} else if (caught instanceof PayloadValidationException validation) {
			switch (validation.getStage()) {
			case COMMAND -> {
				code = "DMP_COMMAND_VALIDATION_FAILED";
				callerStatus = HttpURLConnection.HTTP_INTERNAL_ERROR;
				message = "Mapped command violates the declared input schema";
			}
			case RESPONSE -> {
				code = "DMP_RESPONSE_VALIDATION_FAILED";
				callerStatus = HttpURLConnection.HTTP_BAD_GATEWAY;
				message = "Device response violates the declared output schema";
			}
			default -> {
				code = "DMP_RESPONSE_MAPPING_FAILED";
				callerStatus = HttpURLConnection.HTTP_INTERNAL_ERROR;
				message = "Response mapping failed";
			}
			}
		} else if (caught instanceof ConnectorInvocationException connectorFailure) {
			switch (connectorFailure.getCategory()) {
			case REMOTE -> {
				callerStatus = HttpURLConnection.HTTP_BAD_GATEWAY;
				code = "DMP_DEVICE_ERROR";
				message = "Device invocation failed";
			}
			case TIMEOUT -> {
				callerStatus = HttpURLConnection.HTTP_GATEWAY_TIMEOUT;
				code = "DMP_DEVICE_TIMEOUT";
				message = "Device invocation timed out";
			}
			case TRANSPORT -> {
				callerStatus = HttpURLConnection.HTTP_BAD_GATEWAY;
				code = "DMP_DEVICE_TRANSPORT_ERROR";
				message = "Device transport failed";
			}
			default -> {
				callerStatus = HttpURLConnection.HTTP_INTERNAL_ERROR;
				code = "DMP_CONNECTOR_CONFIGURATION_ERROR";
				message = "Connector configuration failed";
			}
			}
		}
		if (callerStatus == HttpURLConnection.HTTP_BAD_REQUEST && caught instanceof OperationValidationException validation
				&& !validation.getViolations().isEmpty())
			message += ": " + String.join(", ", validation.getViolations());
		String correlationId = exchange.getProperty(SyncOperationProcessor.INVOCATION_ID, String.class);
		LOGGER.warn("Operation failed ({}): {} [{}]", callerStatus, code, correlationId);
		exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, callerStatus);
		exchange.getMessage().setHeader(Exchange.CONTENT_TYPE, "application/json");
		java.util.Map<String, Object> error = new java.util.LinkedHashMap<>();
		error.put("messageType", "Error");
		error.put("code", code);
		error.put("text", message);
		if (correlationId != null) error.put("correlationId", correlationId);
		error.put("timestamp", java.time.Instant.now().toString());
		exchange.getMessage().setBody(JsonCodec.toJson(java.util.Map.of("messages", java.util.List.of(error))));
	}

}
