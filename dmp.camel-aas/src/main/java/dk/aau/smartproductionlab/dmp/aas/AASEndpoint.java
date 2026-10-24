/*******************************************************************************
 * Copyright (C) 2021 the Eclipse BaSyx Authors
 * 
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 * 
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package dk.aau.smartproductionlab.dmp.aas;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.camel.Category;
import org.apache.camel.Consumer;
import org.apache.camel.PollingConsumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.spi.Metadata;
import org.apache.camel.spi.UriEndpoint;
import org.apache.camel.spi.UriParam;
import org.apache.camel.spi.UriPath;
import org.apache.camel.support.DefaultEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dk.aau.smartproductionlab.dmp.aas.http.AasHttp;
import dk.aau.smartproductionlab.dmp.core.util.JsonCodec;
import dk.aau.smartproductionlab.dmp.core.util.JsonPointer;

/**
 * {@code aas:<submodel endpoint>?propertyPath=<idShortPath>}: reads and {@code $value} writes of one element of
 * the AAS repository (SPEC §5.3), through the context's {@link AasHttp}.
 */
@UriEndpoint(firstVersion = "1.0.0-SNAPSHOT", scheme = "aas", title = "AAS", syntax = "aas:name", category = {
		Category.API })
public class AASEndpoint extends DefaultEndpoint {
	private static final Logger logger = LoggerFactory.getLogger(AASEndpoint.class);

	/** The submodel endpoint URL. */
	@UriPath
	@Metadata(required = true)
	private String name;

	/** The element's idShortPath below the submodel; empty for the submodel. */
	@UriParam(defaultValue = "")
	private String propertyPath;

	/**
	 * Optional JSON field to extract from the message body before writing
	 * (the AIMC SinkId out of the Lua blob output object).
	 */
	@UriParam(defaultValue = "")
	private String valuePath;

	/** Resolved AAS key type: how a read is decoded and a write encoded. */
	@UriParam(defaultValue = "Property")
	private String elementType = "Property";

	private volatile String submodelEndpoint;

	public AASEndpoint() {
	}

	public AASEndpoint(String uri, AASComponent component) {
		super(uri, component);
	}

	@Override
	public Producer createProducer() throws Exception {
		return new AASProducer(this);
	}

	@Override
	public Consumer createConsumer(Processor processor) throws Exception {
		return null;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	/** The element's idShortPath below the submodel ({@code Location.x}, {@code Items[0]}); empty for the submodel. */
	public String getPropertyPath() {
		return propertyPath;
	}

	public void setPropertyPath(String propertyPath) {
		this.propertyPath = propertyPath;
	}

	public String getElementType() {
		return elementType;
	}

	public void setElementType(String elementType) {
		this.elementType = elementType;
	}

	public String getValuePath() {
		return valuePath;
	}

	public void setValuePath(String valuePath) {
		this.valuePath = valuePath;
	}

	/** Writes the decoded value (or its {@code valuePath} member) as the element's ValueOnly form. */
	public void setPropertyValue(Object content) throws IOException {
		Object value = extractValuePath(content);
		if (value == JsonPointer.JSON_NULL) value = null;
		if (logger.isDebugEnabled()) logger.debug("AAS write {} <- {}", propertyPath, value);
		AasHttp.of(getCamelContext()).patchValue(getFullProxyUrl(), JsonCodec.toJson(wireValue(value, elementType)));
	}

	/**
	 * The ValueOnly form (AAS Part 2) as BaSyx accepts it: every Property's value as its lexical text, a JSON
	 * string, also inside a collection; BaSyx answers a JSON number or boolean with a 500. Numbers are written
	 * without exponent, so the text is valid for xs:decimal and xs:double alike, and an integral number fits the
	 * integer types. A structured value for a Property is its JSON text.
	 */
	static Object wireValue(Object value, String elementType) {
		if ("SubmodelElementCollection".equalsIgnoreCase(elementType)) return leavesAsText(value);
		if (value instanceof Map<?, ?> || value instanceof List<?>) return JsonCodec.toJson(value);
		return lexical(value);
	}

	private static Object leavesAsText(Object value) {
		if (value instanceof Map<?, ?> members) {
			Map<String, Object> text = new LinkedHashMap<>();
			members.forEach((key, member) -> text.put(String.valueOf(key), leavesAsText(member)));
			return text;
		}
		if (value instanceof List<?> items) {
			List<Object> text = new ArrayList<>(items.size());
			for (Object item : items) text.add(leavesAsText(item));
			return text;
		}
		return value == JsonPointer.JSON_NULL ? null : lexical(value);
	}

	private static Object lexical(Object value) {
		if (value instanceof Boolean) return value.toString();
		if (value instanceof Number number) {
			double asDouble = number.doubleValue();
			if (Double.isNaN(asDouble)) return "NaN";
			if (Double.isInfinite(asDouble)) return asDouble > 0 ? "INF" : "-INF";
			return new BigDecimal(number.toString()).stripTrailingZeros().toPlainString();
		}
		return value;
	}

	/**
	 * When configured, extracts the SinkId field (valuePath) out of the JSON
	 * message body so each sink writes only its own value. The valuePath is a
	 * JSON Pointer (SPEC §5.4), so both a bare key ({@code "PackMLState"})
	 * and a nested pointer ({@code "$.SetPoint"}) work; absent → write the body
	 * as-is (coarse sink).
	 */
	private Object extractValuePath(Object content) {
		if (valuePath == null || valuePath.isBlank() || content == null) {
			return content;
		}
		if (!(content instanceof Map<?, ?>) && !(content instanceof List<?>)) {
			throw new IllegalArgumentException("AAS valuePath requires a decoded object or array payload");
		}
		return JsonPointer.extract(content, valuePath).orElseThrow(() ->
				new IllegalArgumentException("AAS valuePath does not resolve in the decoded payload: " + valuePath));
	}

	public String getSubmodelEndpoint() {
		String endpoint = submodelEndpoint;
		if (endpoint != null) return endpoint;
		endpoint = this.getEndpointBaseUri().substring(4);
		// The endpoint URI is "aas:" + an ABSOLUTE http(s) URL. Camel normalises
		// the scheme-relative remainder, so the captured value can come back with
		// leading slashes ("/http://host" or "//http://host"). Left in place the
		// HTTP client rejects it with "ProtocolException: Target host is not
		// specified". Only a genuine scheme marks the real start, so strip the
		// padding without touching a path-relative endpoint.
		while (endpoint.startsWith("/") && endpoint.contains("://")) {
			endpoint = endpoint.substring(1);
		}
		return submodelEndpoint = endpoint;
	}

	/** The element's URL, its idShortPath percent-encoded (or the submodel's, without one). */
	public String getFullProxyUrl() {
		return AasHttp.elementUrl(getSubmodelEndpoint(), propertyPath);
	}

	@Override
	public PollingConsumer createPollingConsumer() throws Exception {
		return new AASPollingConsumer(this);
	}
}
