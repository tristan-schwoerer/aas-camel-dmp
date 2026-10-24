package dk.aau.smartproductionlab.dmp.executable.regression;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ServiceLoader;

import dk.aau.smartproductionlab.dmp.core.component.DmpRuntimeExtension;
import org.junit.jupiter.api.Test;

public class ConnectorRouteFactoryDiscoveryTest {
	@Test
	public void componentClasspathDiscoversRuntimeExtensions() {
		assertTrue(ServiceLoader.load(DmpRuntimeExtension.class).stream()
				.map(ServiceLoader.Provider::get)
				.anyMatch(extension -> extension instanceof dk.aau.smartproductionlab.dmp.component.health.HealthRouteExtension));
	}
}
