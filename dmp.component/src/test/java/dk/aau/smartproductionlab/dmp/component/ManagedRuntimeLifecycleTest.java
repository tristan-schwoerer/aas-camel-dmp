/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.component;

import static org.junit.jupiter.api.Assertions.*;

import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.camel.builder.RouteBuilder;
import dk.aau.smartproductionlab.dmp.core.component.DmpComponent;
import dk.aau.smartproductionlab.dmp.management.compiler.ConnectorConfigurationFactory;
import dk.aau.smartproductionlab.dmp.management.repository.AasRepositoryClient;
import dk.aau.smartproductionlab.dmp.management.repository.MappingReconciliationService;
import dk.aau.smartproductionlab.dmp.management.repository.RepositoryDiscovery;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

class ManagedRuntimeLifecycleTest {
	@AfterEach void resetStatus() { ManagedRuntimeStatus.reset(); }

	@Test
	void startsEmptyComponentBeforeControlRouteAndStopsAfterFinalScan() throws Exception {
		List<String> lifecycle = new java.util.concurrent.CopyOnWriteArrayList<>();
		HttpServer repository = repository(lifecycle);
		Runtime component = new Runtime();
		component.lifecycle = lifecycle;
		var settings = settings(repository.getAddress().getPort());
		var client = client(repository.getAddress().getPort());
		var discovery = new RepositoryDiscovery(client,
				new ConnectorConfigurationFactory.RuntimeSettings(settings.repositoryUri()));
		var runtime = new ManagedDmpRuntime(settings, component, discovery, (ignored, service) ->
				new RouteBuilder() { @Override public void configure() { } });
		try {
			runtime.start();
			assertEquals(List.of("component-start", "control-route"), List.copyOf(lifecycle).subList(0, 2));
			assertTrue(lifecycle.indexOf("control-route") < lifecycle.indexOf("repository-scan"),
					"control invalidations are accepted before initial membership reads");
			assertEquals(ManagedRuntimeStatus.State.READY, ManagedRuntimeStatus.snapshot().state());
			assertTrue(ManagedRuntimeStatus.snapshot().ready());
			runtime.close();
			assertEquals("component-stop", lifecycle.get(lifecycle.size() - 1));
			assertEquals(ManagedRuntimeStatus.State.STOPPED, ManagedRuntimeStatus.snapshot().state());
		} finally {
			runtime.close();
			repository.stop(0);
		}
	}

	@Test
	void startupFailureLeavesReadinessDownAndCleansTheComponent() throws Exception {
		var observersBefore = observerThreads();
		HttpServer repository = repository();
		Runtime component = new Runtime();
		List<String> lifecycle = new ArrayList<>();
		component.lifecycle = lifecycle;
		component.failControlRoute = true;
		var settings = settings(repository.getAddress().getPort());
		var runtime = new ManagedDmpRuntime(settings, component,
				new RepositoryDiscovery(client(repository.getAddress().getPort()),
						new ConnectorConfigurationFactory.RuntimeSettings(settings.repositoryUri())),
				(managed, service) -> new dk.aau.smartproductionlab.dmp.management.repository.KafkaControlEventRouteBuilder(managed.kafka(), service));
		try {
			assertThrows(IllegalStateException.class, runtime::start);
			assertEquals(List.of("component-start", "control-route", "component-stop"), lifecycle);
			assertEquals(ManagedRuntimeStatus.State.FAILED, ManagedRuntimeStatus.snapshot().state());
			assertFalse(ManagedRuntimeStatus.snapshot().ready());
			assertEquals("control-observer-closed", ManagedRuntimeStatus.asMap().get("controlCategory"));
			org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3)).until(() -> observersBefore.containsAll(observerThreads()));
		} finally { runtime.close(); repository.stop(0); }
	}

	@Test
	void repositoryFailureKeepsManagedReadinessDownWhileRepairServiceStarts() throws Exception {
		HttpServer repository = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		repository.createContext("/repo", exchange -> respond(exchange, 503, "{}"));
		repository.start();
		Runtime component = new Runtime();
		component.lifecycle = new ArrayList<>();
		var settings = settings(repository.getAddress().getPort());
		var runtime = new ManagedDmpRuntime(settings, component,
				new RepositoryDiscovery(client(repository.getAddress().getPort()),
						new ConnectorConfigurationFactory.RuntimeSettings(settings.repositoryUri())),
				(ignored, service) -> new RouteBuilder() { @Override public void configure() { } });
		try {
			runtime.start();
			assertTrue(component.lifecycle.contains("control-route"), "control listener starts so repository repair events are accepted");
			assertEquals(ManagedRuntimeStatus.State.DEGRADED, ManagedRuntimeStatus.snapshot().state());
			assertFalse(ManagedRuntimeStatus.snapshot().ready());
		} finally { runtime.close(); repository.stop(0); }
	}

	@Test
	void actualDmpContextStartsHealthBeforeControlRoutesAndStopsCleanly() throws Exception {
		System.setProperty("dmp.http.port", "0");
		HttpServer repository = repository();
		ActualRuntime component = new ActualRuntime();
		var settings = settings(repository.getAddress().getPort());
		var discovery = new RepositoryDiscovery(client(repository.getAddress().getPort()),
				new ConnectorConfigurationFactory.RuntimeSettings(settings.repositoryUri()));
		var runtime = new ManagedDmpRuntime(settings, component, discovery, (ignored, service) -> new RouteBuilder() {
			@Override public void configure() {
				from("direct:managed-control-test").routeId("managed-control-test").process(exchange -> { });
			}
		});
		try {
			runtime.start();
			assertEquals(ManagedRuntimeStatus.State.READY, ManagedRuntimeStatus.snapshot().state());
			assertTrue(component.routeIds().contains("managed-runtime-status"));
			assertTrue(component.routeIds().contains("managed-control-test"));
			runtime.close();
			assertEquals(ManagedRuntimeStatus.State.STOPPED, ManagedRuntimeStatus.snapshot().state());
			assertTrue(component.routeIds().isEmpty());
		} finally { runtime.close(); repository.stop(0); }
	}

	@Test void invalidationDuringInitialScanIsTrackedAndRepairedBeforeReadiness() throws Exception {
		var service = new java.util.concurrent.atomic.AtomicReference<MappingReconciliationService>();
		var firstRead = new java.util.concurrent.atomic.AtomicBoolean(true);
		var reports = new java.util.concurrent.CopyOnWriteArrayList<MappingReconciliationService.Report>();
		var repaired = new java.util.concurrent.CountDownLatch(1);
		HttpServer repository = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		repository.createContext("/repo", exchange -> {
			if (firstRead.getAndSet(false)) service.get().invalidate(null);
			respond(exchange, 200, "{\"result\":[],\"paging_metadata\":{}}");
		});
		repository.start();
		Runtime component = new Runtime();
		component.lifecycle = new java.util.concurrent.CopyOnWriteArrayList<>();
		var settings = settings(repository.getAddress().getPort());
		var runtime = new ManagedDmpRuntime(settings, component, new RepositoryDiscovery(client(repository.getAddress().getPort()),
				new ConnectorConfigurationFactory.RuntimeSettings(settings.repositoryUri())), (ignored, reconciliation) -> {
			service.set(reconciliation);
			reconciliation.setReportListener(report -> {
				reports.add(report);
				ManagedRuntimeStatus.publish(report);
				if (report.complete() && !report.superseded()) repaired.countDown();
			});
			return new RouteBuilder() { @Override public void configure() { } };
		});
		try {
			runtime.start();
			assertTrue(reports.get(0).superseded(), "initial snapshot cannot converge past an accepted event");
			assertTrue(repaired.await(3, java.util.concurrent.TimeUnit.SECONDS));
			assertTrue(ManagedRuntimeStatus.snapshot().ready());
			assertEquals(1L, service.get().acceptedInvalidationCount());
		} finally { runtime.close(); repository.stop(0); }
	}

	@Test void linkageFailureStopsPartiallyStartedComponentAndReportsFailedStartup() throws Exception {
		HttpServer repository = repository();
		Runtime component = new Runtime();
		component.lifecycle = new ArrayList<>();
		var settings = settings(repository.getAddress().getPort());
		var runtime = new ManagedDmpRuntime(settings, component,
				new RepositoryDiscovery(client(repository.getAddress().getPort()),
						new ConnectorConfigurationFactory.RuntimeSettings(settings.repositoryUri())),
				(ignored, service) -> { throw new NoClassDefFoundError("test-only missing runtime client"); });
		try {
			var failure = assertThrows(IllegalStateException.class, runtime::start);
			assertInstanceOf(NoClassDefFoundError.class, failure.getCause());
			assertEquals(List.of("component-start", "component-stop"), component.lifecycle);
			assertEquals(ManagedRuntimeStatus.State.FAILED, ManagedRuntimeStatus.snapshot().state());
			assertFalse(ManagedRuntimeStatus.snapshot().ready());
		} finally { runtime.close(); repository.stop(0); }
	}

	@Test void linkageFailureAddingKafkaControlClosesCreatedObserverAndAdmin() throws Exception {
		var observersBefore = observerThreads();
		HttpServer repository = repository();
		Runtime component = new Runtime();
		component.lifecycle = new ArrayList<>();
		component.failControlLinkage = true;
		var settings = settings(repository.getAddress().getPort());
		var runtime = new ManagedDmpRuntime(settings, component,
				new RepositoryDiscovery(client(repository.getAddress().getPort()),
						new ConnectorConfigurationFactory.RuntimeSettings(settings.repositoryUri())),
				(managed, service) -> new dk.aau.smartproductionlab.dmp.management.repository.KafkaControlEventRouteBuilder(managed.kafka(), service));
		try {
			var failure = assertThrows(IllegalStateException.class, runtime::start);
			assertInstanceOf(NoClassDefFoundError.class, failure.getCause());
			assertEquals(List.of("component-start", "control-route", "component-stop"), component.lifecycle);
			assertEquals(ManagedRuntimeStatus.State.FAILED, ManagedRuntimeStatus.snapshot().state());
			assertEquals("control-observer-closed", ManagedRuntimeStatus.asMap().get("controlCategory"));
			org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3)).until(() -> observersBefore.containsAll(observerThreads()));
		} finally { runtime.close(); repository.stop(0); }
	}

	private static Set<Thread> observerThreads() {
		return Thread.getAllStackTraces().keySet().stream().filter(thread -> thread.isAlive()
				&& (thread.getName().equals("dmp-control-readiness") || thread.getName().contains("dmp-control-readiness-")))
				.collect(java.util.stream.Collectors.toSet());
	}

	private static ManagedRuntimeSettings settings(int port) {
		return new ManagedRuntimeSettings(URI.create("http://127.0.0.1:" + port + "/repo"), null,
				new dk.aau.smartproductionlab.dmp.management.repository.KafkaControlEventRouteBuilder.Settings(
						"127.0.0.1:9092", "aas-control", "dmp-control"), Set.of(),
				8, 8, 2, 10_000, Duration.ofSeconds(1), Duration.ofSeconds(1), 10, 4);
	}

	private static AasRepositoryClient client(int port) {
		return new AasRepositoryClient(URI.create("http://127.0.0.1:" + port + "/repo"),
				Duration.ofSeconds(1), Duration.ofSeconds(1), 10, 4, null);
	}

	private static HttpServer repository() throws Exception {
		return repository(null);
	}

	private static HttpServer repository(List<String> lifecycle) throws Exception {
		HttpServer repository = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		repository.createContext("/repo", exchange -> {
			String path = exchange.getRequestURI().getRawPath();
			if (lifecycle != null && path.equals("/repo/submodels")) lifecycle.add("repository-scan");
			respond(exchange, 200, "{\"result\":[],\"paging_metadata\":{}}");
		});
		repository.start();
		return repository;
	}

	private static void respond(HttpExchange exchange, int status, String body) throws java.io.IOException {
		byte[] bytes = body.getBytes(java.nio.charset.StandardCharsets.UTF_8);
		exchange.getResponseHeaders().set("Content-Type", "application/json");
		exchange.sendResponseHeaders(status, bytes.length);
		exchange.getResponseBody().write(bytes);
		exchange.close();
	}

	private static final class Runtime extends DmpComponent {
		List<String> lifecycle;
		boolean failControlRoute;
		boolean failControlLinkage;
		Runtime() { super(); }
		@Override public void startComponent() { lifecycle.add("component-start"); }
		@Override public void addRuntimeRoutes(RouteBuilder routes) {
			lifecycle.add("control-route");
			if (failControlLinkage) throw new NoClassDefFoundError("test-only control addition linkage failure");
			if (failControlRoute) throw new IllegalStateException("control route unavailable");
		}
		@Override public void stopComponent() { lifecycle.add("component-stop"); }
	}

	private static final class ActualRuntime extends DmpComponent {
		ActualRuntime() { super(); }
		List<String> routeIds() { return camelContext.getRoutes().stream().map(route -> route.getRouteId()).toList(); }
	}
}
