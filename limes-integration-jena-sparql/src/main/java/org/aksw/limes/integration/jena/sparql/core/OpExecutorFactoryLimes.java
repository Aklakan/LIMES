package org.aksw.limes.integration.jena.sparql.core;

import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.main.OpExecutor;
import org.apache.jena.sparql.engine.main.OpExecutorFactory;

public class OpExecutorFactoryLimes
    implements OpExecutorFactory
{
    private static OpExecutorFactoryLimes instance = null;

    public static synchronized OpExecutorFactoryLimes get() {
        if(instance == null) {
            instance = new OpExecutorFactoryLimes();
        }

        return instance;
    }

    @Override
    public OpExecutor create(ExecutionContext execCxt) {
        return new OpExecutorLimes(execCxt);
    }

}
