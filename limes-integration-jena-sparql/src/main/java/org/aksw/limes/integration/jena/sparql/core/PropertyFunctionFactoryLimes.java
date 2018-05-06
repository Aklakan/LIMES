package org.aksw.limes.integration.jena.sparql.core;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.aksw.jena_sparql_api.utils.Vars;
import org.aksw.limes.core.controller.Controller;
import org.aksw.limes.core.controller.ResultMappings;
import org.aksw.limes.core.io.config.Configuration;
import org.aksw.limes.core.io.config.reader.AConfigurationReader;
import org.aksw.limes.core.io.config.reader.xml.XMLConfigurationReader;
import org.aksw.limes.core.io.mapping.AMapping;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingHashMap;
import org.apache.jena.sparql.engine.iterator.QueryIterPlainWrapper;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.pfunction.PropFuncArg;
import org.apache.jena.sparql.pfunction.PropFuncArgType;
import org.apache.jena.sparql.pfunction.PropertyFunction;
import org.apache.jena.sparql.pfunction.PropertyFunctionEval;
import org.apache.jena.sparql.pfunction.PropertyFunctionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

/**
 * Function for parsing a given CSV resource as a stream of JSON objects
 * 
 * By default, the resource will be attempted to parse as EXCEL csv.
 * 
 * {
 *    <schema://url/to/data> limes:exec ("spec" ?s ?p ?o ?w)
 * }
 *
 * @author raven
 *
 */
public class PropertyFunctionFactoryLimes
    implements PropertyFunctionFactory
{
	private static final Logger logger = LoggerFactory.getLogger(PropertyFunctionFactoryLimes.class);

	public static Var requireVar(Node node, String msg) {
		if(!node.isVariable()) {
			throw new RuntimeException(msg);
		}

		return (Var)node;
	}
	
	@Override
    public PropertyFunction create(final String uri)
    {
        return new PropertyFunctionEval(PropFuncArgType.PF_ARG_SINGLE, PropFuncArgType.PF_ARG_EITHER) {
        	
			@Override
		    public QueryIterator execEvaluated(Binding binding, PropFuncArg argSubject,
		            Node predicate, PropFuncArg argObject, ExecutionContext execCtx) {
				QueryIterator result;

                List<Node> argList = argObject.isList() ? argObject.getArgList() : Collections.singletonList(argObject.getArg());

                // The first argument must be the spec, the remaining four are the output variable names
                int n = argList.size();
                if(n == 2 || n == 3) {
                	Var s = requireVar(argSubject.getArg(), "Variable expected for link source output");

                	int i = 0;
                	Var o = requireVar(argList.get(i++), "Variable expected for link target output");
                	Var c = n == 3 ? requireVar(argList.get(i++), "Variable expected for confidence output") : null;
                	Node specNode = argList.get(i++);

                	String spec = specNode.getLiteralLexicalForm();
	                
	                ResultMappings mappings;
	                try {
		                Path path = Files.createTempFile("limes-sparql-", ".xml");
		                try {
			                Files.write(path, spec.getBytes(StandardCharsets.UTF_8));
			                AConfigurationReader reader = new XMLConfigurationReader(path.toString());
			                Configuration config = reader.read();
			                mappings = Controller.getMapping(config);
			                //Controller.writeResults(mappings, config);
		                } finally {
		                	Files.delete(path);
		                }
	                } catch(Exception e) {
	                	throw new RuntimeException(e);
	                }

	                AMapping m = mappings.getAcceptanceMapping().getBestOneToNMapping();
	                HashMap<String, HashMap<String, Double>> map = m.getMap();

	                Table<String, String, Double> table = createTable(map);
	                Stream<Binding> bindings = toBindings(binding, table, s, o, c);
	                
//	                BindingFactory.binding(binding, vars.get(1), specNode);
//	                
//	                Var outputVar = (Var)object;

	                result = new QueryIterPlainWrapper(bindings.iterator());
              
//					result = new QueryIterPlainWrapper(
//					    new IteratorClosable<>(
//								jsonObjStream
//					    		.map(rowJsonObj -> E_CsvParse.jsonToNode(rowJsonObj))
//					    		.map(n -> BindingFactory.binding(outputVar, n))
//					    		.iterator(),
//					    in));
                        		                
                } else {
                	throw new RuntimeException("Invalid arguments");
                }

                return result;
			}
        };
    }
	
	
	public static <R, C, V> Table<R, C, V> createTable(Map<R, ? extends Map<C, ? extends V>> map) {
		Table<R, C, V> result = HashBasedTable.create();//Tables.newCustomTable(map, HashMap::new);
		
		for(Entry<R, ? extends Map<C, ? extends V>> e : map.entrySet()) {
			for(Entry<C, ? extends V> f : e.getValue().entrySet()) {
				result.put(e.getKey(), f.getKey(), f.getValue());
			}
		}
		
		return result;
	}

	// The binding root should contain the mapping for the predicate
	public static Stream<Binding> toBindings(Binding root, Table<String, String, ? extends Number> table, Var s, Var o, Var c) {
		Stream<Binding> result = table.cellSet().stream()
			.map(cell -> {
				BindingHashMap r = new BindingHashMap(root);
				r.add(s, NodeFactory.createURI(cell.getRowKey()));
				r.add(o, NodeFactory.createURI(cell.getColumnKey()));
				r.add(c, NodeValue.makeDouble(cell.getValue().doubleValue()).asNode());
				return r;
			});

		return result;
	}
}