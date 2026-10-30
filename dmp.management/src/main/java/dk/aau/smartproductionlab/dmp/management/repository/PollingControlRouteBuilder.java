/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.management.repository;

import java.time.Duration;

import org.apache.camel.builder.RouteBuilder;
import dk.aau.smartproductionlab.dmp.core.execution.DmpPollComponent;

/**
 * Repository changes without events, for a repository that sends none the DMP reads (e.g. BaSyx Go): a full
 * discovery every {@code interval}. A full pass reads every AIMC and what its mappings reference afresh, and an
 * unchanged fingerprint keeps the installed bundle, so only what changed is reinstalled. AAS element sources are
 * polled (the compiler requires their interval, as no events feed them).
 */
public final class PollingControlRouteBuilder extends RouteBuilder {
	public static final String RESCAN_ROUTE = "dmp-management-repository-rescan";

	public record Settings(Duration interval) {
		public Settings {
			if (interval == null || interval.toMillis() < 1000 || interval.compareTo(Duration.ofDays(1)) > 0)
				throw new IllegalArgumentException("the repository rescan interval must be 1 s..1 day");
		}
	}

	private final Settings settings;
	private final MappingReconciliationService reconciliation;

	public PollingControlRouteBuilder(Settings settings, MappingReconciliationService reconciliation) {
		this.settings = java.util.Objects.requireNonNull(settings);
		this.reconciliation = reconciliation;
	}

	@Override public void configure() {
		long period = settings.interval().toMillis();
		// The startup scan happened before; the first rescan follows one interval later.
		from(DmpPollComponent.uri(RESCAN_ROUTE, period) + "&delay=" + period).routeId(RESCAN_ROUTE)
				.process(exchange -> rescan());
	}

	/**
	 * One tick: a full rescan, unless the previous one is still queued or running. Invalidating then would supersede
	 * that pass, and passes longer than the interval would never install anything.
	 */
	void rescan() {
		if (!reconciliation.repairPending()) reconciliation.invalidate(null);
	}
}
