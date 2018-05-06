package org.aksw.limes.integration.jena.sparql.core;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.aksw.jena_sparql_api.util.sparql.syntax.path.PathTransformCopyBase;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.sparql.core.Prologue;
import org.apache.jena.sparql.core.TriplePath;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.path.Path;
import org.apache.jena.sparql.syntax.Element;
import org.apache.jena.sparql.syntax.ElementGroup;
import org.apache.jena.sparql.syntax.ElementPathBlock;
import org.apache.jena.sparql.syntax.syntaxtransform.ElementTransformer;

public class MainLimesIntegrationSparql {
	public static void testAnalyzePaths() {
		String queryStr = new BufferedReader(new InputStreamReader(MainLimesIntegrationSparql.class.getClassLoader().getResourceAsStream("limes-bgp.sparql"))).lines().collect(Collectors.joining("\n"));
		System.out.println(queryStr);
		Query query = QueryFactory.create(queryStr);
		
		ElementGroup e = (ElementGroup)query.getQueryPattern();
		ElementPathBlock el = (ElementPathBlock)e.getElements().get(0);
		List<Triple> triples = el.getPattern().getList().stream().map(TriplePath::asTriple).collect(Collectors.toList());
		Var start = LimesBgpAnalyzer.getConceptVar(triples);
		Map<Var, Path> map = LimesBgpAnalyzer.analyzePaths(start, triples);
		System.out.println(map);
	}
	
	public static void main(String[] args) {
		//testAnalyzePaths();

		
//		String queryStr = new BufferedReader(new InputStreamReader(MainLimesIntegrationSparql.class.getClassLoader().getResourceAsStream("limes-pf.sparql"))).lines().collect(Collectors.joining("\n"));
//		System.out.println(queryStr);
		String queryStr = new BufferedReader(new InputStreamReader(MainLimesIntegrationSparql.class.getClassLoader().getResourceAsStream("limes-service.sparql"))).lines().collect(Collectors.joining("\n"));
		System.out.println(queryStr);

		//RDFConnection conn = null;

		
		Model model = ModelFactory.createDefaultModel();
		Query query = QueryFactory.create(queryStr);

//		Prologue prologue = new Prologue(model);

		Prologue prologue = query.getPrologue();
		Element p1 = query.getQueryPattern();
		Element p2 = ElementTransformer.transform(p1, new ElementTransformLimes(prologue));
		query.setQueryPattern(p2);

		System.out.println("QUERY = " + query);
		
		try(QueryExecution qe = QueryExecutionFactory.create(query, model)) {
			ResultSet rs = qe.execSelect();
			System.out.println(ResultSetFormatter.asText(rs));
			
		}
		//QueryFactory.create(str);
	}
}
