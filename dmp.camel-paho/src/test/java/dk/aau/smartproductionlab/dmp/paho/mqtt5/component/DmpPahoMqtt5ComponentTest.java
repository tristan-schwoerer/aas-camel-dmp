/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.paho.mqtt5.component;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import dk.aau.smartproductionlab.dmp.paho.configuration.MqttPublication;
import dk.aau.smartproductionlab.dmp.paho.configuration.MqttSubscription;
import dk.aau.smartproductionlab.dmp.paho.testing.Mqtt5TestBroker;

class DmpPahoMqtt5ComponentTest {
	private Mqtt5TestBroker broker;
	private CamelContext context;

	@BeforeEach
	void start() throws Exception {
		broker = new Mqtt5TestBroker();
		context = new DefaultCamelContext();
	}

	@AfterEach
	void stop() throws Exception {
		context.close();
		broker.close();
	}

	private String source(String name, String topic) {
		return new MqttSubscription("127.0.0.1", broker.port(), name, topic, null, null).uri(null);
	}

	private String sink(String name, String topic) {
		return new MqttPublication("127.0.0.1", broker.port(), topic, name, null, 1, null).uri(null);
	}

	private static void await(BooleanSupplier condition, String what) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
		while (!condition.getAsBoolean()) {
			if (System.nanoTime() > deadline) throw new AssertionError("timed out waiting until " + what);
			Thread.sleep(20);
		}
	}

	@Test
	void theMappingsOfABrokerShareOneConnection() throws Exception {
		int mappings = 200;
		int threadsBefore = ManagementFactory.getThreadMXBean().getThreadCount();
		context.addRoutes(new RouteBuilder() {
			@Override public void configure() {
				for (int i = 0; i < mappings; i++) from(source("source-" + i, "station/" + i + "/state")).routeId("m" + i).to("mock:station" + i);
				// two mappings reading one topic, also through one and the same endpoint
				from(source("left", "shared/state")).to("mock:left");
				from(source("right", "shared/state")).to("mock:right");
				from(source("left", "shared/state")).to("mock:again");
				from("direct:write").to(sink("sink", "station/command"));
			}
		});
		context.start();
		assertEquals(1, broker.clientIds.size(), "one connection for " + mappings + " mappings: " + broker.clientIds);
		int threads = ManagementFactory.getThreadMXBean().getThreadCount() - threadsBefore;
		assertTrue(threads < 30, mappings + " MQTT sources started " + threads + " threads");
		assertEquals(1, context.getComponent(DmpPahoMqtt5Component.SCHEME, DmpPahoMqtt5Component.class).sessionCount());

		MockEndpoint station = context.getEndpoint("mock:station42", MockEndpoint.class);
		station.expectedBodiesReceived("RUNNING");
		broker.publish("station/42/state", "RUNNING");
		station.assertIsSatisfied(5000);
		assertEquals("station/42/state", station.getExchanges().get(0).getMessage().getHeader("CamelMqttTopic"));

		for (String each : new String[] { "mock:left", "mock:right", "mock:again" })
			context.getEndpoint(each, MockEndpoint.class).expectedMessageCount(1);
		assertEquals(1, broker.publish("shared/state", "IDLE"), "the shared filter is subscribed once");
		MockEndpoint.assertIsSatisfied(context, 5, TimeUnit.SECONDS);

		context.createProducerTemplate().sendBody("direct:write", "{\"go\":true}");
		assertTrue(broker.published.contains("station/command"), "a sink's publish returns once the broker acknowledged it");
	}

	@Test
	void aSourceThatJoinsLaterGetsTheRetainedValueAndTheOthersDoNotGetItAgain() throws Exception {
		var retained = new MqttPublication("127.0.0.1", broker.port(), "line/state", "writer", null, 1, true);
		context.addRoutes(new RouteBuilder() {
			@Override public void configure() {
				from(source("first", "line/state")).to("mock:first");
				from("direct:write").to(retained.uri(null));
			}
		});
		context.start();
		MockEndpoint first = context.getEndpoint("mock:first", MockEndpoint.class);
		first.expectedMessageCount(1);
		context.createProducerTemplate().sendBody("direct:write", "RUNNING");
		first.assertIsSatisfied(5000);

		context.addRoutes(new RouteBuilder() {
			@Override public void configure() {
				from(source("second", "line/state")).to("mock:second");
			}
		});
		MockEndpoint second = context.getEndpoint("mock:second", MockEndpoint.class);
		second.expectedBodiesReceived("RUNNING");
		second.assertIsSatisfied(5000);
		Thread.sleep(200);
		assertEquals(1, first.getReceivedCounter(), "the first source already had the retained value");
	}

	@Test
	void aStringForAJsonSinkIsPublishedAsAJsonString() throws Exception {
		// FanOut wraps a string bound for a JSON sink; it once went out as the wrapper's toString().
		var published = new java.util.concurrent.CompletableFuture<String>();
		broker.onPublish(publication -> published.complete(new String(publication.payload(), java.nio.charset.StandardCharsets.UTF_8)));
		context.addRoutes(new RouteBuilder() {
			@Override public void configure() {
				from("direct:write").to(sink("sink", "station/setpoint"));
			}
		});
		context.start();
		context.createProducerTemplate().sendBody("direct:write",
				new dk.aau.smartproductionlab.dmp.core.util.ConnectorJsonPayload("READY"));
		assertEquals("\"READY\"", published.get(5, TimeUnit.SECONDS));
	}

	@Test
	void largeMessagesArriveWholeWithoutPerByteCosts() throws Exception {
		// Paho's MQTT 5 client read a payload one synchronized byte at a time: at least 70 ms per 2 MiB on Java 21,
		// so 30 of them took over two seconds on its single receiver thread (EVALUATION.md §8.4).
		int messages = 30;
		String payload = "x".repeat(2 * 1024 * 1024);
		context.addRoutes(new RouteBuilder() {
			@Override public void configure() {
				// The probes that find the subscription stay out of the count.
				from(source("camera", "cell/image")).filter(exchange -> exchange.getMessage().getBody(byte[].class).length > 1_000)
						.to("mock:image");
			}
		});
		context.start();
		MockEndpoint image = context.getEndpoint("mock:image", MockEndpoint.class);
		await(() -> {
			try { return broker.publish("cell/image", "probe") > 0; }
			catch (java.io.IOException e) { return false; }
		}, "the source subscribed");
		image.expectedMessageCount(messages);
		long started = System.nanoTime();
		for (int i = 0; i < messages; i++) broker.publish("cell/image", payload);
		image.assertIsSatisfied(10_000);
		long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
		for (var exchange : image.getReceivedExchanges())
			assertEquals(payload.length(), exchange.getMessage().getBody(byte[].class).length, "the payload arrives whole");
		assertTrue(millis < 2_000, messages + " messages of 2 MiB took " + millis + " ms");
	}

	@Test
	void aLostConnectionIsReestablishedWithItsSubscriptions() throws Exception {
		context.addRoutes(new RouteBuilder() {
			@Override public void configure() {
				from(source("watcher", "cell/alarm")).to("mock:alarm");
			}
		});
		context.start();
		MockEndpoint alarm = context.getEndpoint("mock:alarm", MockEndpoint.class);
		alarm.expectedBodiesReceived("ON");
		broker.dropClients();
		// a message reaches the source once the session reconnected and subscribed again
		await(() -> {
			try { return broker.publish("cell/alarm", "ON") > 0; }
			catch (java.io.IOException e) { return false; }
		}, "the session reconnected and subscribed again");
		alarm.assertIsSatisfied(5000);
		assertEquals(2, broker.clientIds.size());
	}

	@Test
	void theLastEndpointClosesTheConnectionAndOtherCredentialsHaveTheirOwn() throws Exception {
		context.getRegistry().bind("alice.user", "alice");
		context.getRegistry().bind("alice.password", "secret");
		String alice = "dmp-paho-mqtt5:cell/door?brokerUrl=tcp://127.0.0.1:" + broker.port() + "&name=alice"
				+ "&userName=#bean:alice.user&password=#bean:alice.password";
		context.addRoutes(new RouteBuilder() {
			@Override public void configure() {
				from(source("anonymous", "cell/door")).routeId("anonymous").to("mock:anonymous");
				from(alice).routeId("alice").to("mock:alice");
			}
		});
		context.start();
		var component = context.getComponent(DmpPahoMqtt5Component.SCHEME, DmpPahoMqtt5Component.class);
		assertEquals(2, component.sessionCount());
		assertEquals(2, broker.clientIds.size());
		assertNotEquals(broker.clientIds.get(0), broker.clientIds.get(1));
		context.getRouteController().stopRoute("anonymous");
		assertEquals(1, component.sessionCount(), "the anonymous session lost its last source");
		context.getRouteController().stopRoute("alice");
		assertEquals(0, component.sessionCount());
	}

	@Test
	void aSourceOfAnUnreachableBrokerStartsAndSubscribesOnceTheBrokerIsUp() throws Exception {
		int port = broker.port();
		broker.close();
		context.addRoutes(new RouteBuilder() {
			@Override public void configure() {
				from(new MqttSubscription("127.0.0.1", port, "late", "late/state", null, null).uri(null)).to("mock:late");
			}
		});
		context.start();
		assertEquals(1, context.getComponent(DmpPahoMqtt5Component.SCHEME, DmpPahoMqtt5Component.class).sessionCount(),
				"the mapping installed; its session keeps trying");
		broker = new Mqtt5TestBroker(port);
		MockEndpoint late = context.getEndpoint("mock:late", MockEndpoint.class);
		late.expectedBodiesReceived("UP");
		await(() -> {
			try { return broker.publish("late/state", "UP") > 0; }
			catch (java.io.IOException e) { return false; }
		}, "the session connected and subscribed");
		late.assertIsSatisfied(5000);
	}
}
