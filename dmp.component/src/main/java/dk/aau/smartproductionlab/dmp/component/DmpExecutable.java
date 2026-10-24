/*******************************************************************************
 * Copyright (C) 2023 the Eclipse BaSyx Authors
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
package dk.aau.smartproductionlab.dmp.component;

import dk.aau.smartproductionlab.dmp.core.component.DmpComponent;
import dk.aau.smartproductionlab.dmp.influxdb2.InfluxSettings;

/**
 * Starts the DMP. All routes are built at runtime from the AAS repository and
 * kept current from AAS/submodel events; there is no file-based configuration.
 *
 * @author danish
 */
public class DmpExecutable {

	private static DmpComponent component;
	private static ManagedDmpRuntime managedRuntime;
	private static boolean shutdownHookRegistered;

	public static void main(String[] args) throws IllegalArgumentException, SecurityException {
		if (args.length != 0) throw new IllegalArgumentException("the DMP takes no arguments; configure it through DMP_MANAGED_* environment variables");
		ManagedDmpRuntime runtime = new ManagedDmpRuntime(ManagedRuntimeSettings.fromEnvironment(System.getenv()),
				InfluxSettings.fromEnvironment(System.getenv()).orElse(null), credentials(System.getenv()));
		runtime.start();
		managedRuntime = runtime;
		component = runtime.component();
		registerManagedShutdownHook();
	}

	/**
	 * The credentials answering the AIDs' security schemes: the JSON file {@code DMP_CREDENTIALS_FILE} (a mounted
	 * secret), read once at start; none without it, so only nosec interfaces compile (PLAN H4).
	 */
	static dk.aau.smartproductionlab.dmp.management.compiler.DmpCredentials credentials(java.util.Map<String, String> environment) {
		String file = environment.get("DMP_CREDENTIALS_FILE");
		if (file == null || file.isBlank()) return dk.aau.smartproductionlab.dmp.management.compiler.DmpCredentials.none();
		try {
			return dk.aau.smartproductionlab.dmp.management.compiler.DmpCredentials.read(java.nio.file.Path.of(file.trim()));
		} catch (java.io.IOException unreadable) {
			throw new IllegalArgumentException("DMP_CREDENTIALS_FILE cannot be read: " + unreadable.getClass().getSimpleName());
		}
	}

	public static DmpComponent getDmpComponent() {
		return component;
	}

	public static synchronized void stopManagedRuntime() {
		ManagedDmpRuntime runtime = managedRuntime;
		managedRuntime = null;
		try {
			if (runtime != null) runtime.close();
		} finally {
			component = null;
		}
	}


	private static synchronized void registerManagedShutdownHook() {
		if (shutdownHookRegistered) return;
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			try { stopManagedRuntime(); } catch (RuntimeException ignored) { }
		}, "dmp-managed-shutdown"));
		shutdownHookRegistered = true;
	}
}
