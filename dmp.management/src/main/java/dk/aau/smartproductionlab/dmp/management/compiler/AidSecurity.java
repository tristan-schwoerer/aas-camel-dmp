/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.management.compiler;

import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.eclipse.digitaltwin.aas4j.v3.model.Key;
import org.eclipse.digitaltwin.aas4j.v3.model.ReferenceElement;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElement;

import dk.aau.smartproductionlab.dmp.aas.model.Aas;
import dk.aau.smartproductionlab.dmp.management.model.WireModel;

/**
 * The security an AID form asks for (AID 1.1 §2.24-2.27) and what the DMP sends to satisfy it (PLAN H4). A form's
 * {@code security} replaces its interface's ({@code EndpointMetadata/security}); each entry references a scheme in
 * the interface's {@code securityDefinitions}, and all of them must be satisfied: {@code combo_sc} adds all of its
 * {@code allOf}, or the first of its {@code oneOf} the DMP can apply. AID requires at least one entry
 * ({@code nosec_sc} for an interface without security), so an empty selection blocks the mapping.
 *
 * <p>The AID describes the scheme, never its secret: the credentials come from the DMP's deployment
 * ({@link DmpCredentials}). Applied now: {@code nosec} everywhere, {@code basic} for HTTP and MQTT, {@code bearer}
 * and {@code apikey} for HTTP. Every other scheme blocks the mapping by name and path.
 */
final class AidSecurity {
	/** A problem at a path relative to the endpoint ({@code /forms/security}, {@code /EndpointMetadata/security}). */
	record Problem(String path, String message) { }

	/** One referenced scheme: its reference key for {@link DmpCredentials}, its idShort, its kind and element. */
	record Scheme(String reference, String idShort, String kind, SubmodelElement element) {
		String term(String name) {
			SubmodelElement child = Aas.child(element, name);
			String value = child == null ? null : Aas.value(child);
			return value == null || value.isBlank() ? null : value.trim();
		}
	}

	/**
	 * What a connector sends: HTTP Basic or MQTT CONNECT credentials, and secret headers and query parameters.
	 * Its secrets are bound in the Camel registry by the connector, never written into a URI.
	 */
	record Applied(String username, String password, Map<String, String> headers, Map<String, String> query) {
		static final Applied NONE = new Applied(null, null, Map.of(), Map.of());
		Applied { headers = Map.copyOf(headers); query = Map.copyOf(query); }
		boolean basic() { return username != null; }
		@Override public String toString() { return "Applied[basic=" + basic() + ", headers=" + headers.keySet() + ", query=" + query.keySet() + "]"; }
	}

	private static final int MAX_COMBO_DEPTH = 4;

	private AidSecurity() { }

	/** The schemes a form must satisfy, after {@code combo_sc}; null when the selection is invalid or unsupported. */
	static List<Scheme> required(WireModel.Interface owner, WireModel.Form form, String aidId,
			WireModel.ProtocolFamily family, List<Problem> problems) {
		SubmodelElement metadata = owner.endpointMetadata().present() ? owner.endpointMetadata().value() : null;
		SubmodelElement formSelection = form == null ? null : Aas.child(form.element(), "security");
		boolean onForm = formSelection != null && !Aas.children(formSelection).isEmpty();
		SubmodelElement selection = onForm ? formSelection : metadata == null ? null : Aas.child(metadata, "security");
		String at = onForm ? "/forms/security" : "/EndpointMetadata/security";
		if (selection == null || Aas.children(selection).isEmpty()) {
			problems.add(new Problem("/EndpointMetadata/security", "AID requires at least one security scheme: an interface "
					+ "without security references a nosec_sc in its securityDefinitions (AID 1.1 §2.25)"));
			return null;
		}
		SubmodelElement definitions = metadata == null ? null : Aas.child(metadata, "securityDefinitions");
		List<Scheme> required = new ArrayList<>();
		int before = problems.size();
		for (SubmodelElement entry : Aas.children(selection)) {
			Scheme scheme = resolve(entry, owner, aidId, definitions, at, problems);
			if (scheme != null) add(scheme, owner, aidId, definitions, family, at, required, problems, 0);
		}
		return problems.size() == before ? List.copyOf(required) : null;
	}

	/** Adds a scheme, or the schemes a {@code combo_sc} stands for. */
	private static void add(Scheme scheme, WireModel.Interface owner, String aidId, SubmodelElement definitions,
			WireModel.ProtocolFamily family, String at, List<Scheme> required, List<Problem> problems, int depth) {
		if (!"combo".equals(scheme.kind())) {
			if (!supported(scheme.kind(), family))
				problems.add(new Problem(at, "security scheme " + scheme.kind() + " (" + scheme.idShort() + ") is not "
						+ "applied to " + family + " by the DMP yet"));
			else required.add(scheme);
			return;
		}
		String path = "/EndpointMetadata/securityDefinitions/" + scheme.idShort();
		if (depth >= MAX_COMBO_DEPTH) { problems.add(new Problem(path, "combo_sc nests too deep")); return; }
		SubmodelElement allOf = Aas.child(scheme.element(), "allOf"), oneOf = Aas.child(scheme.element(), "oneOf");
		if ((allOf == null) == (oneOf == null)) {
			problems.add(new Problem(path, "a combo_sc holds exactly one of allOf and oneOf (AID 1.1 §2.27.6)"));
			return;
		}
		if (allOf != null) {
			for (SubmodelElement entry : Aas.children(allOf)) {
				Scheme member = resolve(entry, owner, aidId, definitions, path + "/allOf", problems);
				if (member != null) add(member, owner, aidId, definitions, family, path + "/allOf", required, problems, depth + 1);
			}
			return;
		}
		List<String> refused = new ArrayList<>();
		for (SubmodelElement entry : Aas.children(oneOf)) {
			List<Problem> trial = new ArrayList<>();
			List<Scheme> chosen = new ArrayList<>();
			Scheme member = resolve(entry, owner, aidId, definitions, path + "/oneOf", trial);
			if (member != null) add(member, owner, aidId, definitions, family, path + "/oneOf", chosen, trial, depth + 1);
			if (trial.isEmpty()) { required.addAll(chosen); return; }
			refused.add(trial.get(0).message());
		}
		problems.add(new Problem(path + "/oneOf", "no alternative of " + scheme.idShort() + " can be applied: " + refused));
	}

	/** The scheme a reference names, which must lie in this interface's securityDefinitions. */
	private static Scheme resolve(SubmodelElement entry, WireModel.Interface owner, String aidId, SubmodelElement definitions,
			String at, List<Problem> problems) {
		List<Key> keys = entry instanceof ReferenceElement reference ? Aas.keys(reference.getValue()) : List.of();
		// Submodel / interface / EndpointMetadata / securityDefinitions / scheme. The scheme is looked up in the
		// interface being compiled, so the AID's id in the first key is not compared.
		int n = keys.size();
		boolean inThisInterface = n == 5 && "Submodel".equals(Aas.keyType(keys.get(0))) && owner.idShort().equals(keys.get(1).getValue())
				&& "EndpointMetadata".equals(keys.get(2).getValue()) && "securityDefinitions".equals(keys.get(3).getValue());
		if (!inThisInterface) {
			problems.add(new Problem(at, "a security entry references a scheme in this interface's securityDefinitions"));
			return null;
		}
		String idShort = keys.get(n - 1).getValue();
		SubmodelElement element = definitions == null ? null : Aas.child(definitions, idShort);
		if (element == null) {
			problems.add(new Problem(at, "security references " + idShort + ", which securityDefinitions does not define"));
			return null;
		}
		SubmodelElement kind = Aas.child(element, "scheme");
		String value = kind == null ? null : Aas.value(kind);
		if (value == null || value.isBlank()) {
			problems.add(new Problem("/EndpointMetadata/securityDefinitions/" + idShort, "a security scheme names its scheme (AID 1.1 §2.27)"));
			return null;
		}
		return new Scheme((aidId == null ? "" : aidId) + "#" + owner.idShort() + "/" + idShort, idShort,
				value.trim().toLowerCase(Locale.ROOT), element);
	}

	private static boolean supported(String kind, WireModel.ProtocolFamily family) {
		boolean http = family == WireModel.ProtocolFamily.HTTP || family == WireModel.ProtocolFamily.IOLINK;
		return switch (kind) {
			case "nosec" -> true;
			case "basic" -> http || family == WireModel.ProtocolFamily.MQTT;
			case "bearer", "apikey" -> http;
			default -> false;
		};
	}

	/** What the connector sends to satisfy {@code required} at {@code target}, from the DMP's credentials. */
	static Applied apply(List<Scheme> required, URI target, DmpCredentials credentials, List<Problem> problems) {
		String username = null, password = null;
		Map<String, String> headers = new LinkedHashMap<>(), query = new LinkedHashMap<>();
		List<String> cookies = new ArrayList<>();
		for (Scheme scheme : required) {
			if ("nosec".equals(scheme.kind())) continue;
			String at = "/EndpointMetadata/securityDefinitions/" + scheme.idShort();
			var found = credentials.find(scheme.reference(), target);
			String in = scheme.term("in") == null ? ("apikey".equals(scheme.kind()) ? "query" : "header")
					: scheme.term("in").toLowerCase(Locale.ROOT);
			if (!Set.of("header", "query", "cookie").contains(in)) {
				problems.add(new Problem(at + "/in", scheme.idShort() + " sends its credentials in " + in + "; the DMP sends header, query or cookie"));
				continue;
			}
			String missing = "the DMP has no credentials for " + scheme.idShort() + " of " + DmpCredentials.endpoint(target)
					+ " (DMP_CREDENTIALS_FILE: endpoint or scheme " + scheme.reference() + ")";
			switch (scheme.kind()) {
			case "basic" -> {
				String name = scheme.term("name");
				if (!"header".equals(in) || name != null && !"authorization".equalsIgnoreCase(name)) {
					problems.add(new Problem(at, "basic credentials go in the Authorization header"));
					continue;
				}
				if (found.isEmpty() || found.get().username() == null || found.get().password() == null) {
					problems.add(new Problem(at, missing + ": username and password"));
					continue;
				}
				username = found.get().username();
				password = found.get().password();
			}
			case "bearer", "apikey" -> {
				boolean bearer = "bearer".equals(scheme.kind());
				String secret = found.map(bearer ? DmpCredentials.Credential::bearerToken : DmpCredentials.Credential::apiKey).orElse(null);
				String name = scheme.term("name") != null ? scheme.term("name") : bearer && "header".equals(in) ? "Authorization" : null;
				if (name == null) { problems.add(new Problem(at + "/name", scheme.idShort() + " names its " + in + " parameter")); continue; }
				if (secret == null) { problems.add(new Problem(at, missing + ": " + (bearer ? "bearerToken" : "apiKey"))); continue; }
				String value = bearer && "header".equals(in) && "authorization".equalsIgnoreCase(name) ? "Bearer " + secret : secret;
				switch (in) {
				case "header" -> headers.put(name, value);
				case "query" -> query.put(name, value);
				default -> cookies.add(name + "=" + value);
				}
			}
			default -> problems.add(new Problem(at, "security scheme " + scheme.kind() + " is not applied by the DMP yet"));
			}
		}
		if (!cookies.isEmpty()) headers.put("Cookie", String.join("; ", cookies));
		if (username != null && headers.keySet().stream().anyMatch("authorization"::equalsIgnoreCase))
			problems.add(new Problem("/EndpointMetadata/security", "two schemes both fill the Authorization header"));
		return new Applied(username, password, headers, query);
	}
}
