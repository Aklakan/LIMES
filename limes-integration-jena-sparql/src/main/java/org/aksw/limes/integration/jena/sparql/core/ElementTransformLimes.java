package org.aksw.limes.integration.jena.sparql.core;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.aksw.jena_sparql_api.utils.ExprUtils;
import org.aksw.limes.integration.jena.sparql.plugin.JenaPluginLimes;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.NodeVisitor;
import org.apache.jena.graph.Node_Concrete;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Prologue;
import org.apache.jena.sparql.core.TriplePath;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.path.Path;
import org.apache.jena.sparql.path.PathWriter;
import org.apache.jena.sparql.syntax.Element;
import org.apache.jena.sparql.syntax.ElementFilter;
import org.apache.jena.sparql.syntax.ElementGroup;
import org.apache.jena.sparql.syntax.ElementPathBlock;
import org.apache.jena.sparql.syntax.ElementService;
import org.apache.jena.sparql.syntax.ElementTriplesBlock;
import org.apache.jena.sparql.syntax.PatternVars;
import org.apache.jena.sparql.syntax.syntaxtransform.ElementTransformCopyBase;
import org.apache.jena.sparql.util.graph.GraphList;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Sets;
import com.google.gson.JsonObject;

class Node_Path
	extends Node_Concrete
{
	protected Path path;
	
	protected Node_Path(Path path) {
		super("path:" + Objects.toString(path));
		this.path = path;
	}

	public Path getPath() {
		return path;
	}

	@Override
	public Object visitWith(NodeVisitor v) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((path == null) ? 0 : path.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Node_Path other = (Node_Path) obj;
		if (path == null) {
			if (other.path != null)
				return false;
		} else if (!path.equals(other.path))
			return false;
		return true;
	}
}

/**
 * Transformation of limes service (syntax) elements
 * to invocations of the limes property function
 * 
 * 
 * @author raven May 6, 2018
 *
 */
public class ElementTransformLimes
	extends ElementTransformCopyBase
{
	protected Prologue prologue;
	
	public ElementTransformLimes(Prologue prologue) {
		super();
		this.prologue = prologue;
	}


	@Override
	public Element transform(ElementService el, Node service, Element elt) {
		
		Element result;
		if(service.isURI() && service.getURI().equals("plugin://limes")) {
		
			ElementGroup eg = (ElementGroup)elt;
			
			List<ElementFilter> filters = new ArrayList<>();
			List<ElementService> services = new ArrayList<>();
	
			for(Element e : eg.getElements()) {
				if(e instanceof ElementService) {
					services.add((ElementService)e);
				} else if(e instanceof ElementFilter) {
					filters.add((ElementFilter)e);
				} else {
					throw new RuntimeException("Unsupported element: " + e);
				}
			}
			
			if(services.size() != 2) {
				throw new RuntimeException("Limes Plugin: Exactly two service elements expected");
			}
			
			// Ensure uniqueness of visible variablbes
			ElementService src = services.get(0);
			ElementService tgt = services.get(1);
			
			Set<Var> srcVars = new HashSet<>(PatternVars.vars(src.getElement()));
			Set<Var> tgtVars = new HashSet<>(PatternVars.vars(tgt.getElement()));
			
			Set<Var> violations = Sets.intersection(srcVars, tgtVars);
			if(!violations.isEmpty()) {
				throw new RuntimeException("There must be no variables common to both service elements - violations: " + violations);
			}
			
			
			ServiceInfo srcInfo = processSource(src, prologue);
			ServiceInfo tgtInfo = processSource(tgt, prologue);

			Map<Var, String> varToString = createVarToStringMap(srcInfo, tgtInfo);
			
			ExprVisitorLimesStringConverter exprConverter = new ExprVisitorLimesStringConverter(prologue, varToString);
			
			
			List<Expr> exprs = filters.stream().map(ElementFilter::getExpr).collect(Collectors.toList());
			Expr expr = ExprUtils.andifyBalanced(exprs);
			
			expr.visit(exprConverter);
			
			String metricStr = exprConverter.getResult();
			
			
			StringBuilder xml = new StringBuilder();
			xml.append("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>\n" + 
					"<!DOCTYPE LIMES SYSTEM \"limes.dtd\">\n" + 
					"<LIMES>\n");
			for(Entry<String, String> e : prologue.getPrefixMapping().getNsPrefixMap().entrySet()) {
				xml.append("<PREFIX><NAMESPACE>" + e.getValue() + "</NAMESPACE><LABEL>" + e.getKey() + "</LABEL></PREFIX>\n");				
			}
			
			xml.append(createXmlString("SOURCE", srcInfo) + "\n");
			xml.append(createXmlString("TARGET", tgtInfo) + "\n");
			xml.append("<METRIC>" + metricStr + "</METRIC>\n");
			
			xml.append("<ACCEPTANCE>\n" + 
					"		<THRESHOLD>0.9</THRESHOLD>\n" + 
					"		<FILE>/tmp/limes-sparql-output.nt</FILE>\n" + 
					"		<RELATION>http://www.w3.org/2002/07/owl#sameAs</RELATION>\n" + 
					"	</ACCEPTANCE>\n" + 
					"	<REVIEW>\n" + 
					"		<THRESHOLD>0.5</THRESHOLD>\n" + 
					"		<FILE>/tmp/limes-sparql-output.nt</FILE>\n" + 
					"		<RELATION>http://www.w3.org/2002/07/owl#sameAs</RELATION>\n" + 
					"	</REVIEW>\n" + 
					"\n" + 
					"	<EXECUTION>\n" + 
					"		<REWRITER>default</REWRITER>\n" + 
					"		<PLANNER>default</PLANNER>\n" + 
					"		<ENGINE>default</ENGINE>\n" + 
					"	</EXECUTION>\n" + 
					"");
			
			xml.append("</LIMES>");
			

			System.out.println("Metric str: " + xml);
			
			//result = new ElementGroup();
			ElementPathBlock e = new ElementPathBlock();
			

	        BasicPattern bgp = new BasicPattern();
	        Node o = GraphList.listToTriples(Arrays.<Node>asList(tgtInfo.conceptVar, Var.alloc("conf"), NodeFactory.createLiteral("" + xml)), bgp);
	        
	        
	        bgp.add(new Triple(srcInfo.conceptVar, NodeFactory.createURI(JenaPluginLimes.ns + "limes"), o));
	        
	        result = new ElementTriplesBlock(bgp);
			
		} else {
			result = super.transform(el, service, elt);
		}
		
		return result;
	}

	public static Map<Var, String> createVarToStringMap(ServiceInfo a, ServiceInfo b) {
				
		Map<Var, String> result = Sets.union(
			injectConstant(a.varToAlias, a.conceptVar).entrySet(),
			injectConstant(b.varToAlias, b.conceptVar).entrySet()
		).stream()
			.collect(Collectors.toMap(Entry::getKey, e -> e.getValue().getKey().getName() + "." + e.getValue().getValue()));

		return result;
	}


	public static <K, V, C> Map<K, Entry<C, V>> injectConstant(Map<K, V> map, C c) {
		Map<K, Entry<C, V>> result = map.entrySet().stream()
				.collect(Collectors.toMap(Entry::getKey, e -> new SimpleEntry<>(c, e.getValue())));
		return result;
	}
	
	class ServiceInfo {
		Var conceptVar;
		Map<Var, Path> varToPath;
		Map<Var, String> varToAlias;
		JsonObject json;
		Map<String, String> properties;
		
		public ServiceInfo(Var conceptVar, Map<Var, Path> varToPath, Map<Var, String> varToAlias, JsonObject json) {
			super();
			this.conceptVar = conceptVar;
			this.varToPath = varToPath;
			this.varToAlias = varToAlias;
			this.json = json;
		}
	}

	// TODO Update method in ElementUtils
    public static <T extends Collection<Triple>> T extractTriples(Element e, T result) {
        if(e instanceof ElementGroup) {
            ElementGroup g = (ElementGroup)e;
            for(Element item : g.getElements()) {
                extractTriples(item, result);
            }
        } else if(e instanceof ElementTriplesBlock) {
            ElementTriplesBlock b = (ElementTriplesBlock)e;
            List<Triple> triples = b.getPattern().getList();
            result.addAll(triples);
        }  else if(e instanceof ElementPathBlock) {
        	ElementPathBlock b = (ElementPathBlock)e;
    		List<Triple> triples = b.getPattern().getList().stream().map(ElementTransformLimes::toTriple).collect(Collectors.toList());
            result.addAll(triples);
        }

        return result;
    }
    
    // This method wraps paths as nodes
    public static Triple toTriple(TriplePath tp) {
    	Triple result = tp.asTriple();
    	
    	if(result == null) {
    		result = new Triple(tp.getSubject(), new Node_Path(tp.getPath()), tp.getObject());
    	}

    	return result;
    }

	public ServiceInfo processSource(ElementService service, Prologue prologue) {
		Element e = service.getElement();
		List<Triple> triples = extractTriples(e, new ArrayList<>()); //ElementUtils.extractTriples(e);
		
		Var conceptVar = LimesBgpAnalyzer.getConceptVar(triples);
		Map<Var, Path> varToPath = LimesBgpAnalyzer.analyzePaths(conceptVar, triples);
		
		// TODO Obtain all prefixes
		
		// Simply allocate new names for each path
		// TODO Make alias generation configurable
		BiMap<Var, String> varToAlias = HashBiMap.create();
		for(Var v : varToPath.keySet()) {
			varToAlias.put(v, v.getName());
		}
		
		Map<String, String> aliasToProperty = varToAlias.inverse().entrySet().stream()
				.collect(Collectors.toMap(Entry::getKey, ev -> {
					Path path = varToPath.get(ev.getValue());
					String r = PathWriter.asString(path, prologue);
					return r;
				}));
		
		
		JsonObject json = new JsonObject();
		
		json.addProperty("var", "?" + conceptVar.getName());
		json.addProperty("id", service.getServiceNode().getURI());
		json.addProperty("endpoint", service.getServiceNode().getURI());
		json.addProperty("pageSize", 2000);
		json.addProperty("restriction", Objects.toString(e));
		
		
		ServiceInfo result = new ServiceInfo(conceptVar, varToPath, varToAlias, json); 

		result.properties = aliasToProperty;
		
		return result;
	}
	
	public static String createXmlString(String tag, ServiceInfo s) {
		StringBuilder sb = new StringBuilder();
		JsonObject json = s.json;
		sb.append("<" + tag + ">\n");
		sb.append("    <ID>" + json.get("id").getAsString() + "</ID>\n");
		sb.append("    <ENDPOINT>" + json.get("endpoint").getAsString() + "</ENDPOINT>\n");
		sb.append("    <VAR>" + json.get("var").getAsString() + "</VAR>\n");
		sb.append("    <PAGESIZE>" + json.get("pageSize").getAsInt() + "</PAGESIZE>\n");
		sb.append("    <RESTRICTION><![CDATA[" + json.get("restriction").getAsString() + "]]></RESTRICTION>\n");
		
		for(Entry<String, String> p : s.properties.entrySet()) {
//			sb.append("<PROPERTY>" + p.getValue().replace("<", "").replace(">", "") + " RENAME " + p.getKey() + "</PROPERTY>\n");
			sb.append("<PROPERTY>" + p.getValue() + " RENAME " + p.getKey() + "</PROPERTY>\n");
		}
		sb.append("</" + tag + ">\n");
		
		return sb.toString();
		//sb.append("    <P>" + json.get("restriction").getAsString() + "</RESTRICTION>");		
//		<ENDPOINT>http://linkedgeodata.org/sparql</ENDPOINT>
//		<VAR>?x</VAR>
//		<PAGESIZE>2000</PAGESIZE>
//		<RESTRICTION>?x a lgdo:RelayBox</RESTRICTION>
//		<PROPERTY>geom:geometry/geos:asWKT RENAME polygon</PROPERTY>
//	</SOURCE>

	}

}
