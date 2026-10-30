package dk.aau.smartproductionlab.dmp.aas.http;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;

import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpServer;

/** A read takes a body up to {@link AasHttp#MAX_READ_BYTES}; a larger one, sized or chunked, fails the read (SPEC §5.3). */
class AasHttpReadLimitTest {

	@Test
	void aBodyAtTheLimitIsReadAndALargerOneFailsTheRead() throws Exception {
		HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		for (String path : new String[] { "/at-limit", "/over", "/over-chunked" }) {
			server.createContext(path, exchange -> {
				byte[] body = new byte[(int) AasHttp.MAX_READ_BYTES + (path.equals("/at-limit") ? 0 : 1)];
				Arrays.fill(body, (byte) 'x');
				exchange.sendResponseHeaders(200, path.equals("/over-chunked") ? 0 : body.length);
				exchange.getResponseBody().write(body);
				exchange.close();
			});
		}
		server.start();
		try (DefaultCamelContext context = new DefaultCamelContext()) {
			AasHttp http = AasHttp.of(context);
			String base = "http://127.0.0.1:" + server.getAddress().getPort();
			assertEquals(AasHttp.MAX_READ_BYTES, http.get(base + "/at-limit", Duration.ofSeconds(10)).length());
			for (String path : new String[] { "/over", "/over-chunked" }) {
				IOException failed = assertThrows(IOException.class, () -> http.get(base + path, Duration.ofSeconds(10)));
				assertTrue(String.valueOf(failed.getCause()).contains("exceeds capacity"), path + ": " + failed.getCause());
			}
		} finally {
			server.stop(0);
		}
	}
}
