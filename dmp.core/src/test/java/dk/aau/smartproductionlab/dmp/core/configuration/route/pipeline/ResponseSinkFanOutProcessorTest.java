package dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import dk.aau.smartproductionlab.dmp.core.util.JsonPointer;
import dk.aau.smartproductionlab.dmp.core.util.ConnectorJsonPayload;
import dk.aau.smartproductionlab.dmp.core.util.JsonCodec;
import org.junit.jupiter.api.Test;

class ResponseSinkFanOutProcessorTest {
	@Test
	void detachedWriteFailureLogsNoRawEndpointOrTransportCauseCredentials() throws Exception {
		var context = new DefaultCamelContext();
		context.start();
		var processor = new ResponseSinkFanOutProcessor(context, "redacted", List.of(), 1, 2);
		var logs = new java.io.ByteArrayOutputStream();
		var stderr = System.err;
		try (var captured = new java.io.PrintStream(logs, true, java.nio.charset.StandardCharsets.UTF_8)) {
			System.setErr(captured);
			dispatch(context, processor, List.of(new ResponseSinkFanOutProcessor.DispatchJob(
					"direct:credential-failure?password=raw-write-secret&unused=encoded%20write%20secret",
					1, "safe-target", false)));
			processor.close();
			assertEquals(1, processor.failedWriteCount());
			assertEquals(1L, processor.metrics().get("failedExtraWrites"));
			assertEquals(0L, processor.metrics().get("extraWriteExecutors"));
			assertFalse(processor.metrics().toString().contains("raw-write-secret"));
			String output = logs.toString(java.nio.charset.StandardCharsets.UTF_8);
			assertTrue(output.contains("response sink group safe-target failed"), "capture must include the actual failure diagnostic");
			assertFalse(output.contains("raw-write-secret"));
			assertFalse(output.contains("encoded%20write%20secret"));
			assertFalse(output.contains("encoded write secret"));
			assertFalse(output.contains("direct:credential-failure"));
		} finally {
			System.setErr(stderr);
			processor.close();
			context.stop();
		}
	}
	@Test
	void drainDeadlineCancelsQueuedJobsAndRetainsRunningWorkUntilExplicitCleanupRetry() throws Exception {
		var context = new DefaultCamelContext();
		var entered = new CountDownLatch(1);
		var release = new CountDownLatch(1);
		var writes = new java.util.concurrent.atomic.AtomicInteger();
		context.addRoutes(new RouteBuilder() {
			@Override public void configure() {
				from("direct:uninterruptible-write").process(exchange -> {
					entered.countDown();
					boolean released = false;
					while (!released) {
						try { released = release.await(3, TimeUnit.SECONDS); }
						catch (InterruptedException ignored) { /* Simulate sink I/O that cannot be cancelled. */ }
					}
					writes.incrementAndGet();
				});
			}
		});
		context.start();
		var processor = new ResponseSinkFanOutProcessor(context, "deadline", List.of(target("old-target", "direct:uninterruptible-write")), 1, 2);
		try {
			var job = new ResponseSinkFanOutProcessor.DispatchJob("direct:uninterruptible-write", 1, "old-target", false);
			dispatch(context, processor, List.of(job));
			assertTrue(entered.await(2, TimeUnit.SECONDS));
			dispatch(context, processor, List.of(job));
			long started = System.nanoTime();
			org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
					() -> processor.drain(java.time.Duration.ofMillis(120)));
			assertTrue(java.time.Duration.ofNanos(System.nanoTime() - started).compareTo(java.time.Duration.ofSeconds(1)) < 0);
			assertEquals(1, processor.pendingWriteCount(), "only already running write retains ownership");
			assertEquals(1, processor.rejectedWriteCount(), "queued cancellation must have an outcome");
			assertEquals(1L, processor.metrics().get("pendingExtraWrites"));
			assertEquals(1L, processor.metrics().get("rejectedExtraWrites"));
			dispatch(context, processor, List.of(job));
			assertEquals(2, processor.rejectedWriteCount());
			assertEquals(2L, processor.metrics().get("rejectedExtraWrites"));
			release.countDown();
			processor.close();
			assertEquals(1, writes.get(), "cancelled queued work must not execute or replay");
			assertEquals(0, processor.pendingWriteCount());
			assertTrue(processor.responseExecutor().isTerminated());
		} finally {
			release.countDown();
			processor.close();
			context.stop();
		}
	}
	@Test
	@SuppressWarnings("unchecked")
	void preparedJobsDeeplyIsolateAliasedBodiesFromCallerAndSibling() throws Exception {
		var context = new DefaultCamelContext();
		var completed = new CountDownLatch(1);
		var sibling = new AtomicReference<Object>();
		context.addRoutes(new RouteBuilder() {
			@Override
			public void configure() {
				from("direct:mutating").process(exchange -> {
					Map<String, Object> body = exchange.getMessage().getBody(Map.class);
					((List<Object>) body.get("values")).set(0, "changed");
					((byte[]) body.get("bytes"))[0] = 9;
					((List<Object>) body.get("array")).set(0, "changed");
				});
				from("direct:sibling").process(exchange -> {
					sibling.set(exchange.getMessage().getBody());
					completed.countDown();
				});
			}
		});
		context.start();
		try {
			try (var processor = new ResponseSinkFanOutProcessor(context, "isolation", List.of(
					target("First", "direct:mutating"),
					target("Second", "direct:sibling")), 1, 4)) {
				Map<String, Object> shared = new LinkedHashMap<>();
				shared.put("values", new ArrayList<>(List.of("original")));
				shared.put("bytes", new byte[] { 1, 2 });
				shared.put("array", new Object[] { "original" });
				var exchange = new DefaultExchange(context);
				exchange.getMessage().setBody(Map.of("Reply", shared, "First", shared, "Second", shared));
				var jobs = processor.prepareJobs(exchange);
				assertNotSame(jobs.get(0).body(), jobs.get(1).body());
				assertNotSame(shared, jobs.get(0).body());
				dispatch(context, processor, jobs);
				assertTrue(completed.await(2, TimeUnit.SECONDS));
				Map<?, ?> second = (Map<?, ?>) sibling.get();
				assertEquals(List.of("original"), shared.get("values"));
				assertEquals(List.of("original"), second.get("values"));
				assertEquals(List.of("original"), second.get("array"));
				assertArrayEquals(new Object[] { "original" }, (Object[]) shared.get("array"));
				assertArrayEquals(new byte[] { 1, 2 }, (byte[]) shared.get("bytes"));
				assertArrayEquals(new byte[] { 1, 2 }, (byte[]) second.get("bytes"));
			}
		} finally {
			context.stop();
		}
	}

	@Test
	void enqueueSnapshotsTheJobBeforeTheWorkerStarts() throws Exception {
		var context = new DefaultCamelContext();
		var release = new CountDownLatch(1);
		var completed = new CountDownLatch(1);
		var delivered = new AtomicReference<Object>();
		context.addRoutes(new RouteBuilder() {
			@Override
			public void configure() {
				from("direct:queued").process(exchange -> {
					delivered.set(exchange.getMessage().getBody());
					completed.countDown();
				});
			}
		});
		context.start();
		try {
			try (var processor = new ResponseSinkFanOutProcessor(context, "queued", List.of(target("Queued", "direct:queued")), 1, 4)) {
				processor.responseExecutor().execute(() -> {
					try {
						if (!release.await(2, TimeUnit.SECONDS)) throw new IllegalStateException("test release timed out");
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					}
				});
				Map<String, Object> body = new LinkedHashMap<>(Map.of("values", new ArrayList<>(List.of("original"))));
				dispatch(context, processor, List.of(new ResponseSinkFanOutProcessor.DispatchJob("direct:queued", body, "Queued", false)));
				((List<?>) body.get("values")).clear();
				release.countDown();
				assertTrue(completed.await(2, TimeUnit.SECONDS));
				assertEquals(Map.of("values", List.of("original")), delivered.get());
			}
		} finally {
			release.countDown();
			context.stop();
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	void detachedExchangeCopiesBodyHeaderAndPropertyTrees() throws Exception {
		try (var context = new DefaultCamelContext()) {
			var original = new DefaultExchange(context);
			Map<String, Object> tree = new LinkedHashMap<>(Map.of("values", new ArrayList<>(List.of("original")),
					"bytes", new byte[] { 1, 2 }));
			original.getMessage().setBody(new ResponseSinkFanOutProcessor.DispatchJob("direct:sink", tree, "Sink", false));
			original.getMessage().setHeader("payload", tree);
			original.setProperty("payload", tree);
			var detached = original.copy();
			ResponseSinkFanOutProcessor.prepareDetachedExchange(detached);
			var detachedJob = (ResponseSinkFanOutProcessor.DispatchJob) detached.getMessage().getBody();
			((List<Object>) ((Map<?, ?>) detachedJob.body()).get("values")).set(0, "body");
			((List<Object>) detached.getMessage().getHeader("payload", Map.class).get("values")).set(0, "header");
			((byte[]) detached.getProperty("payload", Map.class).get("bytes"))[0] = 9;
			assertEquals(List.of("original"), tree.get("values"));
			assertArrayEquals(new byte[] { 1, 2 }, (byte[]) tree.get("bytes"));
		}
	}

	@Test
	void controlledNullIsValidatedAsNullAndRetainsIntentionalJsonBodyAtSend() throws Exception {
		var context = new DefaultCamelContext();
		var completed = new CountDownLatch(1);
		var delivered = new AtomicReference<Object>("not delivered");
		context.addRoutes(new RouteBuilder() {
			@Override
			public void configure() {
				from("direct:null").process(exchange -> {
					delivered.set(exchange.getMessage().getBody());
					completed.countDown();
				});
			}
		});
		context.start();
		try {
			try (var processor = new ResponseSinkFanOutProcessor(context, "null", List.of(
					new ResponseSinkFanOutProcessor.Target("Optional", "direct:null", null, null, null, Map.of("type", "null"), true)))) {
				var exchange = new DefaultExchange(context);
				exchange.getMessage().setBody(Map.of("Optional", JsonPointer.JSON_NULL));
				assertNull(processor.prepareJobs(exchange).get(0).body());
				dispatch(context, processor, processor.prepareJobs(exchange));
				assertTrue(completed.await(2, TimeUnit.SECONDS));
				assertNull(((ConnectorJsonPayload) delivered.get()).value());
				assertEquals("null", JsonCodec.toJson(((ConnectorJsonPayload) delivered.get()).value()));
				assertEquals(0, processor.failedWriteCount());
			}
		} finally {
			context.stop();
		}
	}

	@Test
	void saturatedDetachedWritesRejectWithoutBlockingTheCaller() throws Exception {
		var context = new DefaultCamelContext();
		var entered = new CountDownLatch(1);
		var release = new CountDownLatch(1);
		context.addRoutes(new RouteBuilder() {
			@Override
			public void configure() {
				from("direct:slow").process(exchange -> {
					entered.countDown();
					if (!release.await(2, TimeUnit.SECONDS)) {
						throw new IllegalStateException("test release timed out");
					}
				});
			}
		});
		context.start();
		try {
			try (var processor = new ResponseSinkFanOutProcessor(context, "bounded-test", List.of(
					target("First", "direct:slow"),
					target("Second", "direct:slow")), 1, 0)) {
				assertEquals(1, processor.workerCount());
				assertEquals(0, processor.queueCapacity());
				var exchange = new DefaultExchange(context);
				exchange.getMessage().setBody(Map.of("First", 1, "Second", 2));

				dispatch(context, processor, processor.prepareJobs(exchange));
				assertTrue(entered.await(2, TimeUnit.SECONDS));
				assertEquals(1, processor.rejectedWriteCount());
				assertEquals(0, processor.failedWriteCount());
			}
		} finally {
			release.countDown();
			context.stop();
		}
	}

	@Test
	void closeStopsAdmissionAndDrainsAcceptedJobs() throws Exception {
		var context = new DefaultCamelContext();
		var written = new CountDownLatch(1);
		context.addRoutes(new RouteBuilder() {
			@Override public void configure() {
				from("direct:drain").process(exchange -> written.countDown());
			}
		});
		context.start();
		try {
			try (var processor = new ResponseSinkFanOutProcessor(context, "drain", List.of(target("drain", "direct:drain")), 1, 2)) {
				var job = new ResponseSinkFanOutProcessor.DispatchJob("direct:drain", Map.of("v", 1), "drain", false);
				dispatch(context, processor, List.of(job));
				processor.close();
				assertTrue(written.await(100, TimeUnit.MILLISECONDS));
				assertEquals(0, processor.pendingWriteCount());
				assertTrue(processor.responseExecutor().isTerminated());
				assertEquals(0, processor.rejectedWriteCount());
				dispatch(context, processor, List.of(job));
				assertEquals(1, processor.rejectedWriteCount());
				assertEquals(0, processor.pendingWriteCount());
			}
		} finally {
			context.stop();
		}
	}

	/** Sends the jobs through the processor's detached response-write route steps (Wire Tap, write route). */
	private static void dispatch(org.apache.camel.CamelContext context, ResponseSinkFanOutProcessor processor,
			java.util.List<ResponseSinkFanOutProcessor.DispatchJob> jobs) throws Exception {
		String id = "response-writes-" + System.identityHashCode(processor);
		if (context.getRoute(id + "-dispatch") == null) context.addRoutes(new RouteBuilder() {
			@Override public void configure() {
				processor.configureDispatch(from("direct:" + id + "-dispatch").routeId(id + "-dispatch"), "direct:" + id + "-write");
				processor.configureWrite(from("direct:" + id + "-write").routeId(id + "-write"));
			}
		});
		try (var template = context.createProducerTemplate()) {
			template.sendBody("direct:" + id + "-dispatch", jobs);
		}
	}

	/** A target without value path, payload group, schemas or JSON encoding. */
	private static ResponseSinkFanOutProcessor.Target target(String sinkId, String endpoint) {
		return new ResponseSinkFanOutProcessor.Target(sinkId, endpoint, null, null, null, null, false);
	}
}
