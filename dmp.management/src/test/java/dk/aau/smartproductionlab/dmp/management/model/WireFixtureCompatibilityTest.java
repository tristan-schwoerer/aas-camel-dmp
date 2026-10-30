package dk.aau.smartproductionlab.dmp.management.model;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import dk.aau.smartproductionlab.dmp.core.util.JsonCodec;
import static org.junit.jupiter.api.Assertions.assertFalse;
import org.junit.jupiter.api.Test;

class WireFixtureCompatibilityTest {
	@Test void allRelevantSubmodelCasesMatchTheirDeclaredProjectionOutcome() throws Exception {
		Map<?,?> manifest=(Map<?,?>)JsonCodec.parseJson(resource("manifest.json"));
		List<?> cases=(List<?>)manifest.get("cases");
		assertFalse(cases.isEmpty());
		// Every model aas-model generates projects (fixtures/contracts.py).
		for(Object raw:cases) {
			Map<?,?> contract=(Map<?,?>)raw;
			String aimc=(String)contract.get("aimc");
			String aid=(String)contract.get("aid");
			assertDoesNotThrow(() -> WireProjection.aimc(dk.aau.smartproductionlab.dmp.aas.model.Aas.read(resource(aimc), org.eclipse.digitaltwin.aas4j.v3.model.Submodel.class)),aimc);
			assertDoesNotThrow(() -> WireProjection.aid(dk.aau.smartproductionlab.dmp.aas.model.Aas.read(resource(aid), org.eclipse.digitaltwin.aas4j.v3.model.Submodel.class)),aid);
		}
	}
	private static String resource(String name) throws IOException {
		try(var input=WireFixtureCompatibilityTest.class.getResourceAsStream("/runtime-model-contracts/"+name)) {
			assertNotNull(input,"required Java raw fixture "+name);
			return new String(input.readAllBytes(),StandardCharsets.UTF_8);
		}
	}
}
