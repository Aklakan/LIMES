package org.aksw.limes.integration.jena.sparql.plugin;

import org.aksw.limes.integration.jena.sparql.core.OpExecutorFactoryLimes;
import org.aksw.limes.integration.jena.sparql.core.PropertyFunctionFactoryLimes;
import org.apache.jena.query.ARQ;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.pfunction.PropertyFunctionRegistry;

public class JenaPluginLimes {
    public static final String ns = "plugin://";

    
    public static void init() {	
        QC.setFactory(ARQ.getContext(), OpExecutorFactoryLimes.get());
        
        PropertyFunctionRegistry.get().put(ns + "limes", new PropertyFunctionFactoryLimes());
    }
    
    
//    public static void register() {
//        FunctionRegistry.get().put(ns + "parse", E_CsvParse.class);
//        
//    }
    
    public static void addPrefixes(PrefixMapping pm) {
		pm.setNsPrefix("plugin", ns);
    }

}
