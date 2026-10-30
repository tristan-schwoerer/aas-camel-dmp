/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.component;

import java.net.URI;
import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import dk.aau.smartproductionlab.dmp.core.util.JsonCodec;
import dk.aau.smartproductionlab.dmp.management.repository.KafkaControlEventRouteBuilder;
import dk.aau.smartproductionlab.dmp.management.repository.MqttControlEventRouteBuilder;
import dk.aau.smartproductionlab.dmp.management.repository.PollingControlRouteBuilder;

/**
 * Runtime settings of the DMP, read from DMP_MANAGED_* environment variables. Exactly one way of learning about
 * repository changes is configured: BaSyx Java's Kafka events ({@code kafka}), its MQTT feature ({@code mqtt}), or
 * none, rescanning at an interval instead ({@code poll}, e.g. for BaSyx Go; AAS element sources are then polled).
 */
public record ManagedRuntimeSettings(URI repositoryUri, String authorization,
		KafkaControlEventRouteBuilder.Settings kafka, MqttControlEventRouteBuilder.Settings mqtt, Set<String> aimcIds,
		int maxMappings, int maxQueuedInvalidations, int maxParallelReads, long retryMillis,
		Duration connectTimeout, Duration requestTimeout, int pageSize, int maxPages,
		KafkaSecurity kafkaSecurity, PollingControlRouteBuilder.Settings polling) {
	public ManagedRuntimeSettings(URI repositoryUri, String authorization,
			KafkaControlEventRouteBuilder.Settings kafka, MqttControlEventRouteBuilder.Settings mqtt, Set<String> aimcIds,
			int maxMappings, int maxQueuedInvalidations, int maxParallelReads, long retryMillis,
			Duration connectTimeout, Duration requestTimeout, int pageSize, int maxPages, KafkaSecurity kafkaSecurity) {
		this(repositoryUri, authorization, kafka, mqtt, aimcIds, maxMappings, maxQueuedInvalidations, maxParallelReads,
				retryMillis, connectTimeout, requestTimeout, pageSize, maxPages, kafkaSecurity, null);
	}

	public ManagedRuntimeSettings {
		kafkaSecurity = kafkaSecurity == null ? KafkaSecurity.plaintext() : kafkaSecurity;
		if (repositoryUri == null || !repositoryUri.isAbsolute() || repositoryUri.getHost() == null
				|| repositoryUri.getUserInfo() != null || repositoryUri.getFragment() != null || repositoryUri.getQuery() != null
				|| !("http".equalsIgnoreCase(repositoryUri.getScheme()) || "https".equalsIgnoreCase(repositoryUri.getScheme())))
			throw new IllegalArgumentException("managed AAS repository URI must be credential-free HTTP(S) without a query or fragment");
		if ((kafka != null ? 1 : 0) + (mqtt != null ? 1 : 0) + (polling != null ? 1 : 0) != 1)
			throw new IllegalArgumentException("exactly one event transport, Kafka, MQTT or polling, is required");
		aimcIds = aimcIds == null ? Set.of() : Set.copyOf(aimcIds);
		if (aimcIds.size() > 8192 || aimcIds.stream().anyMatch(String::isBlank))
			throw new IllegalArgumentException("managed AIMC filter must contain at most 8192 nonblank exact IDs");
		if (maxMappings < 1 || maxMappings > 8192) throw new IllegalArgumentException("managed max mappings must be 1..8192");
		if (maxQueuedInvalidations < 1 || maxQueuedInvalidations > 65_536)
			throw new IllegalArgumentException("managed invalidation capacity must be 1..65536");
		if (maxParallelReads < 1 || maxParallelReads > 64) throw new IllegalArgumentException("managed read parallelism must be 1..64");
		if (retryMillis < 100 || retryMillis > Duration.ofDays(1).toMillis())
			throw new IllegalArgumentException("managed retry interval must be 100..86400000 ms");
		bounded(connectTimeout, "connect timeout", 100, 120_000);
		bounded(requestTimeout, "request timeout", 100, 120_000);
		if (pageSize < 1 || pageSize > 1000) throw new IllegalArgumentException("managed page size must be 1..1000");
		if (maxPages < 1 || maxPages > 1000) throw new IllegalArgumentException("managed max pages must be 1..1000");
	}

	/** Kafka transport without SASL. */
	public ManagedRuntimeSettings(URI repositoryUri, String authorization,
			KafkaControlEventRouteBuilder.Settings kafka, Set<String> aimcIds,
			int maxMappings, int maxQueuedInvalidations, int maxParallelReads, long retryMillis,
			Duration connectTimeout, Duration requestTimeout, int pageSize, int maxPages) {
		this(repositoryUri, authorization, kafka, null, aimcIds, maxMappings, maxQueuedInvalidations, maxParallelReads,
				retryMillis, connectTimeout, requestTimeout, pageSize, maxPages, KafkaSecurity.plaintext());
	}

	public String transport() { return kafka != null ? "kafka" : mqtt != null ? "mqtt" : "poll"; }

	/** Whether the repository's change events reach the DMP; without them AAS element sources are polled. */
	public boolean repositoryEvents() { return polling == null; }

	/** Control-plane only. Secrets are never placed in Camel endpoint URIs or diagnostics. */
	public record KafkaSecurity(String protocol, String mechanism, String username, String password) {
		public KafkaSecurity {
			if (!Set.of("PLAINTEXT", "SASL_PLAINTEXT", "SASL_SSL").contains(protocol == null ? "" : protocol))
				throw new IllegalArgumentException("managed Kafka security protocol must be PLAINTEXT, SASL_PLAINTEXT or SASL_SSL");
			if (protocol.equals("PLAINTEXT")) {
				if (mechanism != null || username != null || password != null)
					throw new IllegalArgumentException("managed Kafka SASL credentials require a SASL security protocol");
			} else if (!Set.of("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512").contains(mechanism == null ? "" : mechanism)
					|| username == null || username.isBlank() || password == null || password.isBlank()
					|| username.chars().anyMatch(Character::isISOControl) || password.chars().anyMatch(Character::isISOControl)) {
				throw new IllegalArgumentException("managed Kafka SASL requires PLAIN or SCRAM and nonblank credentials without control characters");
			}
		}
		static KafkaSecurity plaintext() { return new KafkaSecurity("PLAINTEXT", null, null, null); }
		Map<String, Object> clientProperties() {
			if (protocol.equals("PLAINTEXT")) return Map.of();
			String module = mechanism.equals("PLAIN") ? "plain.PlainLoginModule" : "scram.ScramLoginModule";
			return Map.of("security.protocol", protocol, "sasl.mechanism", mechanism, "sasl.jaas.config",
					new org.apache.kafka.common.config.types.Password("org.apache.kafka.common.security." + module
					+ " required username=\"" + quoted(username) + "\" password=\"" + quoted(password) + "\";"));
		}
		private static String quoted(String value) { return value.replace("\\", "\\\\").replace("\"", "\\\""); }
		@Override public String toString() { return "KafkaSecurity[protocol=" + protocol + ", credentials=<redacted>]"; }
	}

	public static ManagedRuntimeSettings fromEnvironment(Map<String, String> environment) {
		URI repository;
		try { repository = URI.create(required(environment, "DMP_MANAGED_AAS_REPOSITORY_URI")); }
		catch (IllegalArgumentException invalid) {
			throw new IllegalArgumentException("DMP_MANAGED_AAS_REPOSITORY_URI must be a credential-free HTTP(S) URI");
		}
		String transport = environment.getOrDefault("DMP_MANAGED_EVENT_TRANSPORT", "kafka").trim().toLowerCase(java.util.Locale.ROOT);
		KafkaControlEventRouteBuilder.Settings kafka = null;
		MqttControlEventRouteBuilder.Settings mqtt = null;
		PollingControlRouteBuilder.Settings polling = null;
		switch (transport) {
		case "kafka" -> kafka = new KafkaControlEventRouteBuilder.Settings(
				required(environment, "DMP_MANAGED_KAFKA_BROKERS"), required(environment, "DMP_MANAGED_CONTROL_TOPIC"),
				required(environment, "DMP_MANAGED_CONTROL_GROUP_ID"));
		case "mqtt" -> {
			URI broker;
			try { broker = URI.create(required(environment, "DMP_MANAGED_MQTT_BROKER_URI")); }
			catch (IllegalArgumentException invalid) { throw new IllegalArgumentException("DMP_MANAGED_MQTT_BROKER_URI must be a URI"); }
			mqtt = new MqttControlEventRouteBuilder.Settings(broker,
					environment.getOrDefault("DMP_MANAGED_MQTT_CLIENT_ID", "dmp-control").trim(),
					environment.get("DMP_MANAGED_MQTT_USERNAME"), environment.get("DMP_MANAGED_MQTT_PASSWORD"),
					environment.getOrDefault("DMP_MANAGED_MQTT_REPOSITORY_ID", "+").trim(),
					longValue(environment, "DMP_MANAGED_MQTT_SESSION_EXPIRY_SECONDS", 86_400));
		}
		case "poll" -> {
			String interval = environment.getOrDefault("DMP_MANAGED_RESCAN_INTERVAL", "PT30S").trim();
			try { polling = new PollingControlRouteBuilder.Settings(Duration.parse(interval)); }
			catch (java.time.format.DateTimeParseException invalid) {
				throw new IllegalArgumentException("DMP_MANAGED_RESCAN_INTERVAL must be an ISO-8601 duration such as PT30S");
			}
		}
		default -> throw new IllegalArgumentException("DMP_MANAGED_EVENT_TRANSPORT must be kafka, mqtt or poll");
		}
		return new ManagedRuntimeSettings(repository, environment.get("DMP_MANAGED_AAS_AUTHORIZATION"),
				kafka, mqtt, aimcIds(environment.get("DMP_MANAGED_AIMC_IDS")), integer(environment, "DMP_MANAGED_MAX_MAPPINGS", 4096),
				integer(environment, "DMP_MANAGED_MAX_INVALIDATIONS", 4096),
				integer(environment, "DMP_MANAGED_MAX_PARALLEL_READS", 4),
				longValue(environment, "DMP_MANAGED_RETRY_INTERVAL_MS", 5_000),
				Duration.ofMillis(longValue(environment, "DMP_MANAGED_CONNECT_TIMEOUT_MS", 5_000)),
				Duration.ofMillis(longValue(environment, "DMP_MANAGED_REQUEST_TIMEOUT_MS", 15_000)),
				integer(environment, "DMP_MANAGED_PAGE_SIZE", 100), integer(environment, "DMP_MANAGED_MAX_PAGES", 100),
				new KafkaSecurity(
						environment.getOrDefault("DMP_MANAGED_KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
						environment.get("DMP_MANAGED_KAFKA_SASL_MECHANISM"), environment.get("DMP_MANAGED_KAFKA_SASL_USERNAME"),
						environment.get("DMP_MANAGED_KAFKA_SASL_PASSWORD")), polling);
	}

	@Override public String toString() {
		return "ManagedRuntimeSettings[repositoryUri=" + repositoryUri + ", authorization=<redacted>"
				+ ", transport=" + transport() + ", aimcFilterCount=" + aimcIds.size() + ", maxMappings=" + maxMappings + "]";
	}

	private static void bounded(Duration value, String name, long minMillis, long maxMillis) {
		if (value == null || value.toMillis() < minMillis || value.toMillis() > maxMillis)
			throw new IllegalArgumentException(name + " must be " + minMillis + ".." + maxMillis + " ms");
	}

	private static String required(Map<String, String> environment, String name) {
		String value = environment.get(name);
		if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " is required when managed mode is enabled");
		return value.trim();
	}

	private static int integer(Map<String, String> environment, String name, int fallback) {
		return environment.containsKey(name) ? Integer.parseInt(environment.get(name).trim()) : fallback;
	}

	private static long longValue(Map<String, String> environment, String name, long fallback) {
		return environment.containsKey(name) ? Long.parseLong(environment.get(name).trim()) : fallback;
	}

	private static Set<String> aimcIds(String raw) {
		if (raw == null || raw.isBlank()) return Set.of();
		Object parsed;
		try { parsed = JsonCodec.parseJson(raw); }
		catch (RuntimeException invalid) { throw new IllegalArgumentException("DMP_MANAGED_AIMC_IDS must be a JSON array of exact IDs"); }
		if (!(parsed instanceof java.util.List<?> list) || list.isEmpty() || list.size() > 8192)
			throw new IllegalArgumentException("DMP_MANAGED_AIMC_IDS must be a nonempty JSON array with at most 8192 exact IDs");
		Set<String> ids = new LinkedHashSet<>();
		for (Object item : list) {
			if (!(item instanceof String id) || id.isBlank() || !ids.add(id))
				throw new IllegalArgumentException("DMP_MANAGED_AIMC_IDS contains a non-string, blank or duplicate ID");
		}
		return Set.copyOf(ids);
	}
}
