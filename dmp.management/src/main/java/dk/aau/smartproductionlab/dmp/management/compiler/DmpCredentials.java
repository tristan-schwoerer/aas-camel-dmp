/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.management.compiler;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * The credentials the DMP answers an AID's security schemes with (PLAN H4). They come from the DMP's deployment,
 * a JSON file ({@code DMP_CREDENTIALS_FILE}, e.g. a Docker secret), never from the AAS, and are keyed by what they
 * authenticate against, so equipment on shared infrastructure needs no entry of its own:
 *
 * <pre>
 * {
 *   "endpoints": { "mqtt://broker:1883": { "username": "dmp", "password": "…" },
 *                  "http://device:8080": { "bearerToken": "…" } },
 *   "schemes":   { "&lt;AID submodel id&gt;#interface_http/apikey_sc": { "apiKey": "…" } }
 * }
 * </pre>
 *
 * A scheme reference (the AID's id, the interface and the scheme's idShort) overrides the endpoint, for a secret
 * only one device has. An endpoint is {@code scheme://host:port}, the port always written.
 */
public final class DmpCredentials {
	/** One entry; the fields a scheme needs: Basic a user name and password, bearer a token, apikey a key. */
	public record Credential(String username, String password, String bearerToken, String apiKey) {
		@Override public String toString() { return "Credential[<redacted>]"; }
	}

	private static final DmpCredentials NONE = new DmpCredentials(Map.of(), Map.of());

	private final Map<String, Credential> endpoints;
	private final Map<String, Credential> schemes;

	private DmpCredentials(Map<String, Credential> endpoints, Map<String, Credential> schemes) {
		this.endpoints = Map.copyOf(endpoints);
		this.schemes = Map.copyOf(schemes);
	}

	/** No credentials: only {@code nosec} interfaces compile. */
	public static DmpCredentials none() { return NONE; }

	/** The credentials of a file in the format above; a malformed file is an error, not an empty set. */
	public static DmpCredentials read(Path file) throws IOException {
		return parse(Files.readString(file));
	}

	static DmpCredentials parse(String json) {
		JsonElement root;
		try {
			root = JsonParser.parseString(json);
		} catch (RuntimeException malformed) {
			// The parser's message can quote the file: no secret in a log.
			throw new IllegalArgumentException("the credentials file is not JSON");
		}
		if (!root.isJsonObject()) throw new IllegalArgumentException("the credentials file is a JSON object");
		Map<String, Credential> endpoints = new LinkedHashMap<>();
		section(root.getAsJsonObject(), "endpoints").forEach((key, entry) -> endpoints.put(endpoint(key), entry));
		Map<String, Credential> schemes = section(root.getAsJsonObject(), "schemes");
		for (String key : root.getAsJsonObject().keySet())
			if (!key.equals("endpoints") && !key.equals("schemes"))
				throw new IllegalArgumentException("unknown credentials section " + key);
		return new DmpCredentials(endpoints, schemes);
	}

	private static Map<String, Credential> section(JsonObject root, String name) {
		Map<String, Credential> entries = new LinkedHashMap<>();
		JsonElement section = root.get(name);
		if (section == null) return entries;
		if (!section.isJsonObject()) throw new IllegalArgumentException("credentials section " + name + " is an object");
		for (var entry : section.getAsJsonObject().entrySet()) {
			if (!entry.getValue().isJsonObject())
				throw new IllegalArgumentException("credentials entry " + entry.getKey() + " is an object");
			JsonObject fields = entry.getValue().getAsJsonObject();
			for (String field : fields.keySet())
				if (!java.util.Set.of("username", "password", "bearerToken", "apiKey").contains(field))
					throw new IllegalArgumentException("unknown field " + field + " in credentials entry " + entry.getKey());
			entries.put(entry.getKey(), new Credential(text(fields, "username"), text(fields, "password"),
					text(fields, "bearerToken"), text(fields, "apiKey")));
		}
		return entries;
	}

	private static String text(JsonObject fields, String name) {
		JsonElement value = fields.get(name);
		return value == null || value.isJsonNull() ? null : value.getAsString();
	}

	/** The credential for a scheme reference, else for the endpoint the scheme protects. */
	public Optional<Credential> find(String schemeReference, URI target) {
		Credential override = schemeReference == null ? null : schemes.get(schemeReference);
		if (override != null) return Optional.of(override);
		return target == null ? Optional.empty() : Optional.ofNullable(endpoints.get(endpoint(target)));
	}

	/** {@code scheme://host:port}, lower case, with the scheme's default port written out. */
	static String endpoint(URI uri) {
		String scheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase(Locale.ROOT);
		int port = uri.getPort() > 0 ? uri.getPort() : switch (scheme) {
			case "http" -> 80;
			case "https" -> 443;
			case "mqtt" -> 1883;
			case "mqtts" -> 8883;
			case "opc.tcp" -> 4840;
			default -> -1;
		};
		String host = uri.getHost() == null ? "" : uri.getHost().toLowerCase(Locale.ROOT);
		return scheme + "://" + host + (port > 0 ? ":" + port : "");
	}

	private static String endpoint(String key) {
		try {
			URI uri = URI.create(key);
			if (uri.getHost() == null || uri.getUserInfo() != null || uri.getRawPath() != null && !uri.getRawPath().isEmpty()
					&& !uri.getRawPath().equals("/"))
				throw new IllegalArgumentException();
			return endpoint(uri);
		} catch (IllegalArgumentException invalid) {
			// The key itself may hold user info: it is not quoted.
			throw new IllegalArgumentException("a credentials endpoint is scheme://host:port, without user info or path");
		}
	}

	@Override public String toString() {
		return "DmpCredentials[endpoints=" + endpoints.keySet() + ", schemes=" + schemes.keySet() + "]";
	}
}
