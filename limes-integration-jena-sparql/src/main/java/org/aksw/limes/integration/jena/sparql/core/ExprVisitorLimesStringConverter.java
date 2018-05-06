package org.aksw.limes.integration.jena.sparql.core;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Stack;
import java.util.stream.Collectors;

import org.apache.jena.sparql.core.Prologue;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprAggregator;
import org.apache.jena.sparql.expr.ExprFunction;
import org.apache.jena.sparql.expr.ExprFunctionOp;
import org.apache.jena.sparql.expr.ExprVar;
import org.apache.jena.sparql.expr.ExprVisitorFunction;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.sse.Tags;

public class ExprVisitorLimesStringConverter
	extends ExprVisitorFunction
{
	protected Prologue prologue;
	protected Map<Var, String> varToString;
	//protected Map<Var, Entry<Var, Path>> varToSrcPath;
	//Map<Var, Entry<Var, Path>> varToSrcPath

	public ExprVisitorLimesStringConverter(Prologue prefixMapping, Map<Var, String> varToString) {
		super();
		this.prologue = prefixMapping;
		this.varToString = varToString;
	}

	protected Stack<String> stack = new Stack<>();
	
	public String getResult() {
		String result = stack.peek();
		return result;
	}
	
	@Override
	public void visit(ExprFunctionOp funcOp) {
		throw new UnsupportedOperationException();		
	}

	@Override
	public void visit(NodeValue nv) {
		String str = nv.asNode().getLiteralLexicalForm();
		stack.push(str);
	}

		
	@Override
	public void visit(ExprVar nv) {
		Var var = nv.asVar();
		String str = Objects.requireNonNull(varToString.get(var), "No mapping for " + var + "; available: " + varToString);
		
		stack.push(str);
	}

	@Override
	public void visit(ExprAggregator eAgg) {
		throw new UnsupportedOperationException();				
	}

	@Override
	protected void visitExprFunction(ExprFunction func) {
		List<String> args = func.getArgs().stream()
				.map(this::accept)
				.collect(Collectors.toList());
		
		String fnIri = func.getFunctionIRI();
		String prefix = "plugin://";
		if(fnIri != null && fnIri.startsWith(prefix)) {
			String fnName = fnIri.substring(prefix.length());
			stack.push(fnName + "(" + args.stream().collect(Collectors.joining(", ")) + ")");
		} else {
		
			String opName = func.getOpName();
			if(opName != null) {
				switch(opName) {
				case Tags.symAnd:
					stack.push("AND(" + args.stream().collect(Collectors.joining(", ")) + ")");
					break;
				case Tags.symOr:
					stack.push("OR(" + args.stream().collect(Collectors.joining(", ")) + ")");
					break;
				case Tags.symLE:
				case Tags.symLT:
					stack.push(args.get(0) + "|" + args.get(1));
					break;
				default:
					throw new UnsupportedOperationException("Could not handle func " + func);
				}
			} else {
				throw new RuntimeException("Should not happen " + func);
			}
		}
	}

	public String accept(Expr expr) {
		expr.visit(this);
		String result = stack.pop();
		return result;
	}
}
