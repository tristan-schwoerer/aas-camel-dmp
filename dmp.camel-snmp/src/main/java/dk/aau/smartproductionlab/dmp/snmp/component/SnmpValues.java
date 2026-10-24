/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.snmp.component;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.snmp4j.smi.Counter64;
import org.snmp4j.smi.Integer32;
import org.snmp4j.smi.IpAddress;
import org.snmp4j.smi.Null;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.OctetString;
import org.snmp4j.smi.UnsignedInteger32;
import org.snmp4j.smi.Variable;

/**
 * SNMP values as JSON and back: integers (Integer32, Counter32, Gauge32, TimeTicks, Counter64)
 * as numbers, an OCTET STRING as its text when printable and as hex ({@code 0a:1b:…}) otherwise,
 * an IpAddress and an OBJECT IDENTIFIER as their dotted text.
 */
public final class SnmpValues {
	private SnmpValues() {
	}

	public static Object json(Variable value) {
		if (value == null || value instanceof Null) return null;
		if (value instanceof Integer32 integer) return integer.getValue();
		if (value instanceof UnsignedInteger32 unsigned) return unsigned.getValue();
		if (value instanceof Counter64 counter) {
			long raw = counter.getValue();
			return raw >= 0 ? raw : new BigInteger(Long.toUnsignedString(raw));
		}
		if (value instanceof OctetString octets) {
			String text = utf8Text(octets.getValue());
			return text != null ? text : octets.toHexString();
		}
		if (value instanceof IpAddress address) return address.getInetAddress().getHostAddress();
		if (value instanceof OID oid) return oid.toDottedString();
		return value.toString();
	}

	/** A JSON value as an SMI value of the same type as {@code template}, the object's current value. */
	public static Variable variable(Object json, Variable template) {
		if (json == null) throw new IllegalArgumentException("an SNMP object cannot be set to null");
		if (template instanceof Integer32) return new Integer32(Math.toIntExact(integral(json, Integer.MIN_VALUE, Integer.MAX_VALUE)));
		if (template instanceof UnsignedInteger32) {
			Variable copy = (Variable) template.clone();
			((UnsignedInteger32) copy).setValue(integral(json, 0, 0xFFFFFFFFL));
			return copy;
		}
		if (template instanceof Counter64) return new Counter64(integral(json, Long.MIN_VALUE, Long.MAX_VALUE));
		if (template instanceof IpAddress) return new IpAddress(text(json));
		if (template instanceof OID) return new OID(text(json));
		if (template instanceof OctetString)
			return new OctetString((json instanceof String text ? text : String.valueOf(json)).getBytes(java.nio.charset.StandardCharsets.UTF_8));
		throw new IllegalArgumentException("SNMP type " + template.getSyntaxString() + " cannot be set");
	}

	private static long integral(Object json, long min, long max) {
		if (!(json instanceof Number number)) throw new IllegalArgumentException("an integer is required, not " + json);
		BigInteger value;
		try {
			value = new BigDecimal(number.toString()).toBigIntegerExact();
		} catch (ArithmeticException fraction) {
			throw new IllegalArgumentException("an integer is required, not " + json);
		}
		if (value.compareTo(BigInteger.valueOf(min)) < 0 || value.compareTo(BigInteger.valueOf(max)) > 0)
			throw new IllegalArgumentException(json + " is outside [" + min + ", " + max + "]");
		return value.longValue();
	}

	private static String text(Object json) {
		if (json instanceof String text) return text;
		throw new IllegalArgumentException("a string is required, not " + json);
	}

	/**
	 * Octets as text when they are valid UTF-8 without control characters (tab and line breaks allowed);
	 * otherwise null, and the value is its hex form. snmp4j's own test admits ASCII only, so {@code café}
	 * came out as hex.
	 */
	static String utf8Text(byte[] octets) {
		String text;
		try {
			text = java.nio.charset.StandardCharsets.UTF_8.newDecoder()
					.onMalformedInput(java.nio.charset.CodingErrorAction.REPORT)
					.onUnmappableCharacter(java.nio.charset.CodingErrorAction.REPORT)
					.decode(java.nio.ByteBuffer.wrap(octets)).toString();
		} catch (java.nio.charset.CharacterCodingException binary) {
			return null;
		}
		for (int i = 0; i < text.length(); i++) {
			char c = text.charAt(i);
			if (Character.isISOControl(c) && c != '\t' && c != '\n' && c != '\r') return null;
		}
		return text;
	}
}
