package org.aksw.limes.integration.jena.sparql.core;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.aksw.commons.jena.graph.GraphVar;
import org.aksw.commons.jena.graph.GraphVarImpl;
import org.aksw.commons.jena.jgrapht.PseudoGraphJenaGraph;
import org.aksw.jena_sparql_api.util.sparql.syntax.path.PathTransformInvert;
import org.aksw.jena_sparql_api.util.sparql.syntax.path.PathTransformer;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.path.P_Link;
import org.apache.jena.sparql.path.P_ReverseLink;
import org.apache.jena.sparql.path.P_Seq;
import org.apache.jena.sparql.path.Path;
import org.jgrapht.Graph;
import org.jgrapht.GraphPath;
import org.jgrapht.alg.interfaces.ShortestPathAlgorithm.SingleSourcePaths;
import org.jgrapht.alg.shortestpath.BellmanFordShortestPath;

public class LimesBgpAnalyzer {
	public static Var getConceptVar(Collection<Triple> ts) {
		// By convention, the first variable in a subject position marks the concept variable
		// Note: Further conventions, such as naming ones, could be added in the future
		Var result = null;
		for(Triple t : ts) {
			Node s = t.getSubject();
			if(s.isVariable()) {
				result = (Var)s;
				break;
			}
		}

		return result;
	}
	
	
	public static Map<Var, Path> analyzePaths(Node start, Collection<Triple> triples) {
		GraphVar g = new GraphVarImpl();
		for(Triple t : triples) {
			g.add(t);
		}
		Set<Var> vars = g.getVarToNode().keySet();
		
		Map<Var, Path> result = analyzeReachablePaths(g, start, vars);
		return result;
	}
	
	//Collection<Triple> ts
	public static <N extends Node> Map<N, Path> analyzeReachablePaths(org.apache.jena.graph.Graph graph, Node start, Collection<N> targets) {
		Map<N, Path> result = new HashMap<>();
		Graph<Node, Triple> g = new PseudoGraphJenaGraph(graph);
		
		BellmanFordShortestPath<Node, Triple> bf = new BellmanFordShortestPath<>(g);
		SingleSourcePaths<Node, Triple> pathFinder = bf.getPaths(start);
		for(N target : targets) {
			GraphPath<Node, Triple> path = pathFinder.getPath(target);
			
			Path p = createPath(path.getStartVertex(), path.getEdgeList());

			if(p != null) {
				result.put(target, p);
			}
		}
		
		return result;
	}
	
	
    public static Path createPath(Node s, List<Triple> triples) {
        Path result = null;
    	for(Triple t : triples) {
            Node o;
            boolean reverse;
            if(t.getSubject().equals(s)) {
                reverse = false;
                o = t.getObject();
            } else if(t.getObject().equals(s)) {
                o = t.getSubject();
                reverse = true;
            } else {
            	throw new RuntimeException("Encountered disconnected triple although a path was expected");
            }

            Path contrib;
            Node rawP = t.getPredicate();
            if(rawP instanceof Node_Path) {
            	Path p = ((Node_Path)rawP).getPath();
                contrib = reverse ? PathTransformer.transform(p, new PathTransformInvert()) : p;
            } else {
                contrib = reverse ? new P_ReverseLink(rawP) : new P_Link(rawP);
            }
            
            result = result == null ? contrib : new P_Seq(result, contrib);

            s = o;
        }

    	return result;
    }
}
