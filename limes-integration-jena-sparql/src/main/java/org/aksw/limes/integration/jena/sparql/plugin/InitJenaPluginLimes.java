package org.aksw.limes.integration.jena.sparql.plugin;

import org.apache.jena.system.JenaSubsystemLifecycle;

public class InitJenaPluginLimes implements JenaSubsystemLifecycle {
	public void start() {
		JenaPluginLimes.init();
	}

	@Override
	public void stop() {
	}
}
