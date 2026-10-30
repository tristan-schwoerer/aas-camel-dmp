/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.management.compiler;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The path an Operation is invoked at in the AAS API,
 * {@code /submodels/{base64url(submodel id)}/submodel-elements/{idShortPath}/invoke}: a submodel id is globally
 * unique and an idShortPath unique within its submodel, so no two Operations share it. aas-model writes it into
 * the {@code invocationDelegation} URL ({@code invocation_authoring.operation_path}); the DMP serves exactly it.
 */
public final class OperationPaths {
	private static final Pattern INVOKE = Pattern.compile("/submodels/([^/]+)/submodel-elements/([^/]+)/invoke");

	private OperationPaths() { }

	/** The canonical path: the id base64url-encoded without padding, the idShortPath percent-encoded but its dots. */
	public static String invokePath(String submodelId, String idShortPath) {
		return "/submodels/" + Base64.getUrlEncoder().withoutPadding().encodeToString(submodelId.getBytes(StandardCharsets.UTF_8))
				+ "/submodel-elements/" + encode(idShortPath) + "/invoke";
	}

	/** Whether {@code rawPath} invokes the Operation at {@code idShortPath} in {@code submodelId}; padding is optional. */
	public static boolean sameInvokePath(String rawPath, String submodelId, String idShortPath) {
		Matcher matcher = INVOKE.matcher(rawPath == null ? "" : rawPath);
		if (!matcher.matches()) return false;
		try {
			String identifier = new String(Base64.getUrlDecoder().decode(matcher.group(1)), StandardCharsets.UTF_8);
			return identifier.equals(submodelId) && decode(matcher.group(2)).equals(idShortPath);
		} catch (IllegalArgumentException malformed) {
			return false;
		}
	}

	/** RFC 3986 percent-encoding of everything but unreserved characters, as Python's {@code quote(safe=".")}. */
	private static String encode(String value) {
		StringBuilder encoded = new StringBuilder();
		for (byte b : value.getBytes(StandardCharsets.UTF_8)) {
			char c = (char) (b & 0xff);
			if (c < 0x80 && (Character.isLetterOrDigit(c) || "-._~".indexOf(c) >= 0)) encoded.append(c);
			else encoded.append('%').append(String.format("%02X", b & 0xff));
		}
		return encoded.toString();
	}

	private static String decode(String value) {
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		for (int i = 0; i < value.length(); i++) {
			char c = value.charAt(i);
			if (c == '%') {
				if (i + 2 >= value.length()) throw new IllegalArgumentException("truncated percent-encoding");
				bytes.write(Integer.parseInt(value.substring(i + 1, i + 3), 16));
				i += 2;
			} else bytes.write(c);
		}
		return bytes.toString(StandardCharsets.UTF_8);
	}
}
