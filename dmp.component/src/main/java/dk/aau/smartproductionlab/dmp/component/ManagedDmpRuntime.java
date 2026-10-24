/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.component;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.camel.builder.RouteBuilder;
import dk.aau.smartproductionlab.dmp.aas.http.AasHttp;
import dk.aau.smartproductionlab.dmp.core.component.DmpComponent;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.RoutesConfiguration;
import dk.aau.smartproductionlab.dmp.influxdb2.InfluxSettings;
import dk.aau.smartproductionlab.dmp.influxdb2.component.DmpInfluxDb2Component;
import dk.aau.smartproductionlab.dmp.management.compiler.ConnectorConfigurationFactory.RuntimeSettings;
import dk.aau.smartproductionlab.dmp.management.compiler.DmpCredentials;
import dk.aau.smartproductionlab.dmp.management.repository.AasRepositoryClient;
import dk.aau.smartproductionlab.dmp.management.repository.KafkaControlEventRouteBuilder;
import dk.aau.smartproductionlab.dmp.management.repository.MqttControlEventRouteBuilder;
import dk.aau.smartproductionlab.dmp.management.repository.MappingReconciliationService;
import dk.aau.smartproductionlab.dmp.management.repository.RepositoryDiscovery;

/**
 * The repository-managed runtime: one full scan at startup, then repository events over Kafka or MQTT keep
 * the installed mappings current.
 */
public final class ManagedDmpRuntime implements AutoCloseable {
	@FunctionalInterface interface ControlRouteFactory {
		RouteBuilder create(ManagedRuntimeSettings settings, MappingReconciliationService reconciliation);
	}
	private final ManagedRuntimeSettings settings;
	private final InfluxSettings influx;
	private final DmpCredentials credentials;
	private final DmpComponent component;
	private final RepositoryDiscovery discovery;
	private final ControlRouteFactory controlRoutes;
	private final AtomicBoolean closed = new AtomicBoolean();
	private ManagedControlReadiness controlReadiness;
	private MappingReconciliationService reconciliation;
	private boolean started;

	public ManagedDmpRuntime(ManagedRuntimeSettings settings) { this(settings, null); }

	/** {@code influx} is the InfluxDB of TimeSeries segments, or {@code null} when none is configured. */
	public ManagedDmpRuntime(ManagedRuntimeSettings settings, InfluxSettings influx) {
		this(settings, influx, DmpCredentials.none());
	}

	/** {@code credentials} answer the AIDs' security schemes (PLAN H4); {@link DmpCredentials#none()} for nosec only. */
	public ManagedDmpRuntime(ManagedRuntimeSettings settings, InfluxSettings influx, DmpCredentials credentials) {
		this(settings, influx, credentials, new DmpComponent(), discovery(settings, influx, credentials),
				(managedSettings, service) -> managedSettings.mqtt() != null
						? new MqttControlEventRouteBuilder(managedSettings.mqtt(), service)
						: new KafkaControlEventRouteBuilder(managedSettings.kafka(), service));
	}

	ManagedDmpRuntime(ManagedRuntimeSettings settings, DmpComponent component,
			RepositoryDiscovery discovery, ControlRouteFactory controlRoutes) {
		this(settings, null, component, discovery, controlRoutes);
	}

	ManagedDmpRuntime(ManagedRuntimeSettings settings, InfluxSettings influx, DmpComponent component,
			RepositoryDiscovery discovery, ControlRouteFactory controlRoutes) {
		this(settings, influx, DmpCredentials.none(), component, discovery, controlRoutes);
	}

	ManagedDmpRuntime(ManagedRuntimeSettings settings, InfluxSettings influx, DmpCredentials credentials, DmpComponent component,
			RepositoryDiscovery discovery, ControlRouteFactory controlRoutes) {
		this.settings = java.util.Objects.requireNonNull(settings);
		this.influx = influx;
		this.credentials = credentials == null ? DmpCredentials.none() : credentials;
		this.component = java.util.Objects.requireNonNull(component);
		this.discovery = java.util.Objects.requireNonNull(discovery);
		this.controlRoutes = java.util.Objects.requireNonNull(controlRoutes);
	}

	public synchronized void start() {
		if (closed.get()) throw new IllegalStateException("managed runtime is closed");
		if (started) return;
		ManagedRuntimeStatus.beginManaged();
		try {
			component.startComponent();
			// Data-plane reads, writes and event enrichment carry the authorization discovery uses.
			if (settings.authorization() != null) AasHttp.of(component.runtimeContext()).authorize(settings.authorization());
			if (influx != null) component.runtimeContext()
					.getComponent(DmpInfluxDb2Component.SCHEME, DmpInfluxDb2Component.class).configure(influx);
			reconciliation = new MappingReconciliationService(component, discovery, new RuntimeSettings(settings.repositoryUri(), influx, credentials),
					settings.maxMappings(), settings.maxQueuedInvalidations(), settings.retryMillis());
			MappingStateLog mappingStateLog = new MappingStateLog();
			reconciliation.setReportListener(report -> {
				mappingStateLog.publish(report);
				ManagedRuntimeStatus.publish(report);
			});
			ManagedRuntimeStatus.trackReconciliation(reconciliation, component);
			RouteBuilder controlRoute = controlRoutes.create(settings, reconciliation);
			if (controlRoute instanceof KafkaControlEventRouteBuilder kafkaRoute) {
				if (!settings.kafkaSecurity().clientProperties().isEmpty()) {
					kafkaRoute.addLifecycleInterceptor(new org.apache.camel.builder.RouteBuilderLifecycleStrategy() {
						@Override public void beforeConfigure(RouteBuilder builder) {
							for (String uri : java.util.List.of(kafkaRoute.endpointUri(), kafkaRoute.valueEndpointUri())) {
								var endpoint = builder.getContext().getEndpoint(uri, org.apache.camel.component.kafka.KafkaEndpoint.class);
								endpoint.getConfiguration().setAdditionalProperties(settings.kafkaSecurity().clientProperties());
							}
						}
					});
				}
				controlReadiness = new ManagedControlReadiness(settings.kafka(), kafkaRoute.clientId(), kafkaRoute::consumerReady,
						settings.kafkaSecurity());
				ManagedRuntimeStatus.trackControlReadiness(controlReadiness);
			} else if (controlRoute instanceof MqttControlEventRouteBuilder mqttRoute) {
				controlReadiness = new ManagedControlReadiness(mqttRoute::consumerReady);
				ManagedRuntimeStatus.trackControlReadiness(controlReadiness);
			}
			component.addRuntimeRoutes(controlRoute);
			ManagedRuntimeStatus.controlRouteStarted();
			if (controlReadiness != null) controlReadiness.start();
			// The one full scan; afterwards only repository events change desired state.
			reconciliation.reconcileNow();
			reconciliation.start();
			started = true;
		} catch (Exception | LinkageError failure) {
			ManagedRuntimeStatus.failed("managed-startup-failed");
			cleanupAfterStartFailure();
			throw new IllegalStateException("managed DMP runtime failed to start", failure);
		}
	}

	public DmpComponent component() { return component; }
	public MappingReconciliationService reconciliation() { return reconciliation; }

	@Override public synchronized void close() {
		if (!closed.compareAndSet(false, true)) return;
		ManagedRuntimeStatus.stopping();
		Throwable failure = null;
		try { if (controlReadiness != null) controlReadiness.close(); }
		catch (RuntimeException | LinkageError e) { failure = e; }
		if (reconciliation != null) {
			try { reconciliation.close(); }
			catch (RuntimeException | LinkageError e) { if (failure == null) failure = e; else failure.addSuppressed(e); }
		}
		try { component.stopComponent(); }
		catch (RuntimeException | LinkageError e) { if (failure == null) failure = e; else failure.addSuppressed(e); }
		ManagedRuntimeStatus.stopped();
		if (failure != null) throw new IllegalStateException("managed DMP shutdown was incomplete", failure);
	}

	private void cleanupAfterStartFailure() {
		try { if (controlReadiness != null) controlReadiness.close(); }
		catch (RuntimeException | LinkageError ignored) { }
		try { if (reconciliation != null) reconciliation.close(); }
		catch (RuntimeException | LinkageError ignored) { }
		try { component.stopComponent(); }
		catch (RuntimeException | LinkageError ignored) { }
	}

	private static RepositoryDiscovery discovery(ManagedRuntimeSettings settings, InfluxSettings influx, DmpCredentials credentials) {
		var repository = new AasRepositoryClient(settings.repositoryUri(), settings.connectTimeout(),
				settings.requestTimeout(), settings.pageSize(), settings.maxPages(), settings.authorization());
		return new RepositoryDiscovery(repository, new RuntimeSettings(settings.repositoryUri(), influx, credentials),
				settings.maxParallelReads(), settings.aimcIds());
	}
}
