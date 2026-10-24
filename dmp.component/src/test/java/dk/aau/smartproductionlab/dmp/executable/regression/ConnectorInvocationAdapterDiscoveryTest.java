package dk.aau.smartproductionlab.dmp.executable.regression;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ServiceLoader;
import java.util.List;

import dk.aau.smartproductionlab.dmp.core.configuration.route.core.SynchronousInvocationAdapter;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.SynchronousSourceReadAdapter;
import org.junit.jupiter.api.Test;

public class ConnectorInvocationAdapterDiscoveryTest {
	@Test
	public void componentClasspathDiscoversSynchronousConnectorAdapters() {
		List<SynchronousInvocationAdapter> adapters = ServiceLoader
				.load(SynchronousInvocationAdapter.class).stream()
				.map(ServiceLoader.Provider::get).toList();
		assertTrue(adapters.stream().anyMatch(a -> a.supports("dmp-milo-client:test")));
		assertTrue(adapters.stream().anyMatch(a -> a.supports("dmp-paho-mqtt5:command")));
		assertTrue(adapters.stream().anyMatch(a -> a.supports("http://device/action")));
		assertFalse(adapters.stream().anyMatch(a -> a.supports("dmp-plc4x:modbus-tcp://device")),
				"a PLC4X action trigger is the dmp-plc4x producer's acknowledged tag write");
	}

	@Test
	public void componentClasspathDiscoversFreshSourceReadAdapters() {
		List<SynchronousSourceReadAdapter> adapters = ServiceLoader
				.load(SynchronousSourceReadAdapter.class).stream()
				.map(ServiceLoader.Provider::get).toList();
		assertTrue(adapters.stream().anyMatch(a -> a.supports("dmp-milo-client:test")));
	}
}
