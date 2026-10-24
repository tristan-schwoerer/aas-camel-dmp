/*******************************************************************************
 * Copyright (C) 2026 the Eclipse BaSyx Authors
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
package dk.aau.smartproductionlab.dmp.lua.sandbox;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

import org.luaj.vm2.Globals;

/**
 * An immutable, eagerly compiled Lua source supplied by a managed runtime. Identical sources share one compiled
 * prototype, whichever mapping they belong to (SPEC §8); its chunk is named {@value #CHUNK_NAME}, and the
 * {@code lua:} producer names the mapping in its errors.
 */
public final class LuaCompiledSource {
	/** The chunk name of every compiled source: a shared prototype belongs to no one mapping. */
	static final String CHUNK_NAME = "transformation";
	/** Compiled prototypes by the SHA-256 of their source; a prototype no source holds any more can be collected. */
	private static final java.util.Map<String, java.lang.ref.WeakReference<CompiledLuaScript>> PROTOTYPES = new java.util.HashMap<>();

	private final byte[] bytes;
	private final String diagnosticIdentity;
	private final String contentIdentity;
	private final CompiledLuaScript compiledScript;

	private LuaCompiledSource(byte[] bytes, String diagnosticIdentity, String contentIdentity,
			CompiledLuaScript compiledScript) {
		this.bytes = bytes;
		this.diagnosticIdentity = diagnosticIdentity;
		this.contentIdentity = contentIdentity;
		this.compiledScript = compiledScript;
	}

	/** Validates the byte limit and Lua syntax before returning the resource. */
	public static LuaCompiledSource fromBytes(byte[] sourceBytes, String diagnosticIdentity, long maxScriptBytes) {
		return fromBytes(sourceBytes, diagnosticIdentity, maxScriptBytes, LuaCompiledSource::shared);
	}

	/** The compiled prototype of {@code source}, shared with every identical source. */
	private static CompiledLuaScript shared(String source, String diagnosticIdentity) {
		String key = sha256(source.getBytes(StandardCharsets.UTF_8));
		synchronized (PROTOTYPES) {
			var cached = PROTOTYPES.get(key);
			CompiledLuaScript script = cached == null ? null : cached.get();
			if (script != null) return script;
			PROTOTYPES.values().removeIf(reference -> reference.get() == null);
		}
		CompiledLuaScript compiled = CompiledLuaScript.compile(source, CHUNK_NAME);
		synchronized (PROTOTYPES) {
			var cached = PROTOTYPES.get(key);
			CompiledLuaScript script = cached == null ? null : cached.get();
			if (script != null) return script;
			PROTOTYPES.put(key, new java.lang.ref.WeakReference<>(compiled));
			return compiled;
		}
	}

	static LuaCompiledSource fromBytes(byte[] sourceBytes, String diagnosticIdentity, long maxScriptBytes,
			ScriptCompiler compiler) {
		Objects.requireNonNull(sourceBytes, "sourceBytes");
		if (diagnosticIdentity == null || diagnosticIdentity.isBlank()) {
			throw new IllegalArgumentException("Lua diagnostic identity must not be blank");
		}
		if (maxScriptBytes < 0) {
			throw new IllegalArgumentException("maxScriptBytes must not be negative");
		}
		if (sourceBytes.length > maxScriptBytes) {
			throw new IllegalArgumentException("Lua source exceeds maxScriptBytes=" + maxScriptBytes
					+ " (" + sourceBytes.length + " bytes): " + diagnosticIdentity);
		}

		byte[] immutableBytes = sourceBytes.clone();
		String source;
		try {
			source = StandardCharsets.UTF_8.newDecoder()
					.onMalformedInput(CodingErrorAction.REPORT)
					.onUnmappableCharacter(CodingErrorAction.REPORT)
					.decode(ByteBuffer.wrap(immutableBytes)).toString();
		} catch (CharacterCodingException e) {
			throw new IllegalArgumentException("Lua source is not valid UTF-8: " + diagnosticIdentity, e);
		}
		String sha256 = sha256(immutableBytes);
		CompiledLuaScript compiled = compiler.compile(source, diagnosticIdentity);
		return new LuaCompiledSource(immutableBytes, diagnosticIdentity, sha256, compiled);
	}

	@FunctionalInterface
	interface ScriptCompiler {
		CompiledLuaScript compile(String source, String diagnosticIdentity);
	}

	public byte[] bytes() {
		return bytes.clone();
	}

	/** Exact retained source payload length, excluding the compiled graph and JVM overhead. */
	public long retainedSourceBytes() { return bytes.length; }

	public String diagnosticIdentity() {
		return diagnosticIdentity;
	}

	/** SHA-256 of the exact source bytes; suitable for effective fingerprints. */
	public String contentIdentity() {
		return contentIdentity;
	}

	/** The compiled prototype, for tests. */
	CompiledLuaScript compiled() {
		return compiledScript;
	}

	public void runChunk(Globals globals) {
		CompiledLuaScript.runChunk(compiledScript.prototype(), globals);
	}

	private static String sha256(byte[] bytes) {
		try {
			return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalStateException("SHA-256 unavailable", e);
		}
	}
}
