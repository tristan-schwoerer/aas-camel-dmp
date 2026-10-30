/*******************************************************************************
 * Copyright (C) 2026 the Eclipse BaSyx Authors
 * 
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons whom the Software is furnished to do so, subject to
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
package dk.aau.smartproductionlab.dmp.opcua.canonical;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Map;

import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the JSON value of OPC UA built-in values (SPEC §5.2).
 */
class OpcUaCanonicalTest {

	@Test
	void scalarsKeepTheirJsonType() {
		assertEquals(42.5, OpcUaCanonical.jsonValue(42.5));
		assertEquals("idle", OpcUaCanonical.jsonValue("idle"));
		assertEquals(Boolean.TRUE, OpcUaCanonical.jsonValue(true));
		assertNull(OpcUaCanonical.jsonValue(null));
	}

	@Test
	void byteArrayValueBecomesBase64() {
		assertEquals("AQID", OpcUaCanonical.jsonValue(new byte[] { 1, 2, 3 }));
	}

	@Test
	void decodedStructuresUnionsAndEnumerationsAreJsonObjectsAndNumbers() {
		var type = (org.eclipse.milo.opcua.sdk.core.typetree.DataType) java.lang.reflect.Proxy.newProxyInstance(
				getClass().getClassLoader(), new Class<?>[] { org.eclipse.milo.opcua.sdk.core.typetree.DataType.class },
				(proxy, method, args) -> method.getName().equals("toString") ? "Stub" : null);
		var enumType = (org.eclipse.milo.opcua.sdk.core.typetree.DataType) java.lang.reflect.Proxy.newProxyInstance(
				getClass().getClassLoader(), new Class<?>[] { org.eclipse.milo.opcua.sdk.core.typetree.DataType.class },
				(proxy, method, args) -> method.getName().equals("getDataTypeDefinition")
						? new org.eclipse.milo.opcua.stack.core.types.structured.EnumDefinition(
								new org.eclipse.milo.opcua.stack.core.types.structured.EnumField[] {
										new org.eclipse.milo.opcua.stack.core.types.structured.EnumField(2L, null, null, "Running") }) : null);
		var members = new java.util.LinkedHashMap<String, Object>();
		members.put("Name", "Cake");
		members.put("Limits", new org.eclipse.milo.opcua.stack.core.types.structured.Range(0.0, 10.0));
		members.put("Mode", new org.eclipse.milo.opcua.sdk.core.types.DynamicEnumType(enumType, 2));
		members.put("Steps", new Integer[] { 1, 2 });
		assertEquals(Map.of("Name", "Cake", "Limits", Map.of("Low", 0.0, "High", 10.0), "Mode", 2, "Steps", java.util.List.of(1, 2)),
				OpcUaCanonical.jsonValue(new Variant(new org.eclipse.milo.opcua.sdk.core.types.DynamicStructType(type, members))));
		assertEquals(Map.of("Fault", 7), OpcUaCanonical.jsonValue(new org.eclipse.milo.opcua.sdk.core.types.DynamicUnionType(type,
				new org.eclipse.milo.opcua.sdk.core.types.DynamicUnionType.UnionValue("Fault", 7))));
		assertNull(OpcUaCanonical.jsonValue(new org.eclipse.milo.opcua.sdk.core.types.DynamicUnionType(type)), "a union holding no field");
		assertEquals(1, OpcUaCanonical.jsonValue(org.eclipse.milo.opcua.stack.core.types.enumerated.NodeClass.Object));
	}
}
