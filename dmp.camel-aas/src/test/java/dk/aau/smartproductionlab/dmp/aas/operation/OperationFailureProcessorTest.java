package dk.aau.smartproductionlab.dmp.aas.operation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.ConnectorInvocationException;
import dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline.PayloadValidationException;
import dk.aau.smartproductionlab.dmp.core.util.JsonCodec;
import org.junit.jupiter.api.Test;

class OperationFailureProcessorTest {
	private static final String SECRET = "private-user:private-password%3Aencoded";
	private static final String URI = "opcua://" + SECRET + "@localhost:4840?password=private-password";

	@Test
	void arbitraryMessagesAndNestedCauseTextNeverReachCallerOrLogs() throws Exception {
		assertDiagnostic(new IllegalArgumentException(URI, new IllegalStateException(SECRET)),
				500, 500, "DMP_OPERATION_FAILED", "Operation failed");
		assertDiagnostic(null, 504, 504, "DMP_DEVICE_TIMEOUT", "Device invocation timed out");
	}

	@Test
	void realRejectedCredentialBearingCamelEndpointsHaveSafePublicDiagnostics() throws Exception {
		try (var camel = new DefaultCamelContext()) {
			for (String uri : List.of(URI, "paho-mqtt5:command?brokerUrl=tcp://localhost:1883"
					+ "&userName=private-user&password=private-password%3Aencoded&notAnOption=true")) {
				Exception rejected = assertThrows(Exception.class, () -> camel.getEndpoint(uri));
				assertDiagnostic(rejected, 500, 500, "DMP_OPERATION_FAILED", "Operation failed");
			}
		}
	}

	@Test
	void typedConnectorMessagesCodesAndDetailsAreNotTrustedPublicText() throws Exception {
		var categories = ConnectorInvocationException.Category.values();
		var codes = Map.of(ConnectorInvocationException.Category.REMOTE, "DMP_DEVICE_ERROR",
				ConnectorInvocationException.Category.TIMEOUT, "DMP_DEVICE_TIMEOUT",
				ConnectorInvocationException.Category.TRANSPORT, "DMP_DEVICE_TRANSPORT_ERROR",
				ConnectorInvocationException.Category.CONFIGURATION, "DMP_CONNECTOR_CONFIGURATION_ERROR");
		var messages = Map.of(ConnectorInvocationException.Category.REMOTE, "Device invocation failed",
				ConnectorInvocationException.Category.TIMEOUT, "Device invocation timed out",
				ConnectorInvocationException.Category.TRANSPORT, "Device transport failed",
				ConnectorInvocationException.Category.CONFIGURATION, "Connector configuration failed");
		for (var category : categories) {
			var failure = new ConnectorInvocationException(category, SECRET, URI,
					Map.of("cause", URI, "deviceError", Map.of("password", SECRET), "endpoint", URI));
			int status = category == ConnectorInvocationException.Category.TIMEOUT ? 504
					: category == ConnectorInvocationException.Category.CONFIGURATION ? 500 : 502;
			assertDiagnostic(failure, 500, status, codes.get(category), messages.get(category));
			assertDiagnostic(new org.apache.camel.CamelExecutionException(URI, null, failure),
					500, status, codes.get(category), messages.get(category));
		}
	}

	@Test
	void typedValidationViolationsAndMessagesAreNotEchoed() throws Exception {
		for (var stage : PayloadValidationException.Stage.values()) {
			String code = stage == PayloadValidationException.Stage.COMMAND ? "DMP_COMMAND_VALIDATION_FAILED"
					: stage == PayloadValidationException.Stage.RESPONSE ? "DMP_RESPONSE_VALIDATION_FAILED"
					: "DMP_RESPONSE_MAPPING_FAILED";
			String message = stage == PayloadValidationException.Stage.COMMAND ? "Mapped command violates the declared input schema"
					: stage == PayloadValidationException.Stage.RESPONSE ? "Device response violates the declared output schema"
					: "Response mapping failed";
			int status = stage == PayloadValidationException.Stage.RESPONSE ? 502 : 500;
			assertDiagnostic(new PayloadValidationException(stage, URI, List.of(SECRET)), 500, status, code, message);
			assertDiagnostic(new OperationValidationException(code, status, URI, List.of(SECRET)),
					500, status, code, message);
		}
		assertDiagnostic(new OperationValidationException(SECRET, 200, URI, List.of(SECRET)),
				500, 500, "DMP_OPERATION_VALIDATION_FAILED", "Operation validation failed");
	}

	@Test
	void aRequestTheCallerGotWrongNamesTheOmittedInputs() throws Exception {
		assertDiagnostic(new OperationValidationException("DMP_OPERATION_VALIDATION_FAILED", 400,
				"Operation request omits the required input Speed", List.of("Speed")),
				500, 400, "DMP_OPERATION_VALIDATION_FAILED", "Operation validation failed: Speed");
	}

	private void assertDiagnostic(Exception failure, int configuredStatus, int expectedStatus,
			String expectedCode, String expectedMessage) throws Exception {
		// The reactor's active SLF4J provider is SimpleLogger, not the legacy Log4j binding.
		var logs = new ByteArrayOutputStream();
		PrintStream previous = System.err;
		try (var capture = new PrintStream(logs, true, StandardCharsets.UTF_8);
				var camel = new DefaultCamelContext()) {
			System.setErr(capture);
			var exchange = new DefaultExchange(camel);
			exchange.setProperty(Exchange.EXCEPTION_CAUGHT, failure);
			var context = OperationContext.builder(SECRET, URI)
				.callerInputSourcePaths(Map.of(SECRET, "$"))
				.callerReplySinkId(SECRET)
				.commandSinkIds(List.of(SECRET))
				.build();
			new OperationFailureProcessor(context, configuredStatus).process(exchange);
			assertEquals(expectedStatus, exchange.getMessage().getHeader(Exchange.HTTP_RESPONSE_CODE));
			assertEquals(true, exchange.getProperty(OperationPayloads.OPERATION_DELEGATION_FAILED));
			String result = exchange.getMessage().getBody(String.class);
			assertEquals(Map.of("code", expectedCode, "error", expectedMessage), Delegation.error(result));
			Map<?, ?> message = (Map<?, ?>) ((List<?>) ((Map<?, ?>) JsonCodec.parseJson(result)).get("messages")).get(0);
			assertEquals("Error", message.get("messageType"), "the AAS API's Result: " + result);
			assertTrue(message.get("timestamp") instanceof String, result);
			String logged = logs.toString(StandardCharsets.UTF_8);
			assertTrue(logged.contains("Operation failed (" + expectedStatus + "): " + expectedCode),
					"the active logger must be captured, not silently ignored");
			assertFalse(logged.contains("private-user"));
			assertFalse(logged.contains("private-password"));
			assertFalse(logged.contains("encoded"));
			assertFalse(logged.contains("opcua://"));
			assertFalse(logged.contains("paho-mqtt5:"));
		} finally {
			System.setErr(previous);
		}
	}
}
