package dk.aau.smartproductionlab.dmp.paho.mqtt5.component;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class MqttReplyRegistryTest {
	@Test
	void duplicatesNeverShareFuturesAcrossOwnersOrRegistries() {
		var wire = new MqttReplyRegistry.WireState(10);
		var first = new MqttReplyRegistry(wire);
		var second = new MqttReplyRegistry(wire);
		Object a = new Object();
		Object b = new Object();
		var pending = first.register(a, "broker", "reply", "authored");
		assertThrows(IllegalStateException.class, () -> first.register(a, "broker", "reply", "authored"));
		assertThrows(IllegalStateException.class, () -> first.register(b, "broker", "reply", "authored"));
		assertThrows(IllegalStateException.class, () -> second.register(b, "broker", "reply", "authored"));
		assertFalse(second.complete("broker", "reply", "authored", "foreign"));
		assertFalse(pending.isDone());
		assertTrue(first.complete("broker", "reply", "authored", "owned"));
		assertEquals("owned", pending.getNow(null));
	}

	@Test
	void brokerAndChannelAreCheckedWithoutTrimmingAuthoredCorrelation() throws Exception {
		var registry = new MqttReplyRegistry(new MqttReplyRegistry.WireState(10));
		Object owner = new Object();
		var pending = registry.register(owner, "broker", "reply", " authored ");
		var channel = registry.register(owner, "broker", "other-reply", " authored ");
		var broker = registry.register(owner, "other-broker", "reply", " authored ");
		assertFalse(registry.complete("broker", "reply", "authored", "{}"));
		assertFalse(pending.isDone());
		assertTrue(registry.complete("broker", "reply", " authored ", "{\"State\":\"SUCCESS\"}"));
		assertTrue(pending.isDone());
		assertFalse(channel.isDone());
		assertFalse(broker.isDone());
		registry.cancelOwner(owner);
	}

	@Test
	void cancellationIsPerOwnerAndAFinishedCallFreesItsReservation() {
		var registry = new MqttReplyRegistry(new MqttReplyRegistry.WireState(2));
		Object oldOwner = new Object();
		Object newOwner = new Object();
		var old = registry.register(oldOwner, "broker", "reply", "same");
		var independent = registry.register(newOwner, "broker", "reply", "independent");
		assertThrows(IllegalStateException.class, () -> registry.register(newOwner, "broker", "reply", "full"),
				"capacity bounds the calls in flight");
		registry.cancelOwner(oldOwner);
		assertTrue(old.isCancelled());
		assertFalse(independent.isDone(), "stopping one owner cannot cancel another");
		assertFalse(registry.complete("broker", "reply", "same", "late old generation"));
		var current = registry.register(newOwner, "broker", "reply", "same");
		registry.unregister(oldOwner, "broker", "reply", "same", old);
		assertEquals(2, registry.pendingCount(), "old cleanup must not remove the new call");
		assertTrue(registry.complete("broker", "reply", "same", "current"));
		assertEquals("current", current.getNow(null));
		assertTrue(registry.complete("broker", "reply", "independent", "ok"));
		assertEquals(0, registry.pendingCount());
	}
}
