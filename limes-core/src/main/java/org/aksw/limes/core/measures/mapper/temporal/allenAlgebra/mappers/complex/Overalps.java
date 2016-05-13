package org.aksw.limes.core.measures.mapper.temporal.allenAlgebra.mappers.complex;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.TreeMap;
import java.util.TreeSet;

import org.aksw.limes.core.io.cache.Cache;
import org.aksw.limes.core.io.mapping.Mapping;
import org.aksw.limes.core.io.mapping.MemoryMapping;
import org.aksw.limes.core.measures.mapper.IMapper.Language;
import org.aksw.limes.core.measures.mapper.temporal.allenAlgebra.mappers.AllenAlgebraMapper;
import org.aksw.limes.core.measures.mapper.temporal.allenAlgebra.mappers.atomic.BeginBegin;
import org.aksw.limes.core.measures.mapper.temporal.allenAlgebra.mappers.atomic.EndBegin;
import org.aksw.limes.core.measures.mapper.temporal.allenAlgebra.mappers.atomic.EndEnd;

import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.Set;

public class Overalps extends AllenAlgebraMapper {

    public Overalps() {
	// (BB1 & EE1) \ (EB0 U EB1)
	this.getRequiredAtomicRelations().add(1);
	this.getRequiredAtomicRelations().add(7);
	this.getRequiredAtomicRelations().add(4);
	this.getRequiredAtomicRelations().add(5);

    }

    @Override
    public String getName() {
	return "OverlapBefore: (BB1 & EE1) \\ (EB0 U EB1)";
    }

    @Override
    public Mapping getMapping(ArrayList<TreeMap<String, Set<String>>> maps) {
	Mapping m = new MemoryMapping();
	TreeMap<String, Set<String>> mapBB1 = maps.get(0);
	TreeMap<String, Set<String>> mapEE1 = maps.get(1);

	TreeMap<String, Set<String>> mapEB0 = maps.get(2);
	TreeMap<String, Set<String>> mapEB1 = maps.get(3);

	for (Map.Entry<String, Set<String>> entryBB1 : mapBB1.entrySet()) {
	    long start = System.currentTimeMillis();
	    // get targets from EB1
	    String instanceBB1 = entryBB1.getKey();
	    Set<String> setBB1 = entryBB1.getValue();

	    Set<String> setEE1 = mapEE1.get(instanceBB1);
	    if (setEE1 == null)
		setEE1 = new TreeSet<String>();

	    Set<String> intersection = AllenAlgebraMapper.intersection(setBB1, setEE1);

	    Set<String> setEB0 = mapEB0.get(instanceBB1);
	    Set<String> setEB1 = mapEB1.get(instanceBB1);
	    if (setEB0 == null)
		setEB0 = new TreeSet<String>();
	    if (setEB1 == null)
		setEB1 = new TreeSet<String>();
	    Set<String> union = AllenAlgebraMapper.union(setEB0, setEB1);

	    Set<String> difference = AllenAlgebraMapper.difference(intersection, union);

	    if (!difference.isEmpty()) {
		for (String targetInstanceUri : difference) {
		    m.add(instanceBB1, targetInstanceUri, 1);
		}
	    }
	}
	return m;
    }
    @Override
    public Mapping getMapping(Cache source, Cache target, String sourceVar, String targetVar, String expression,
	    double threshold) {
	ArrayList<TreeMap<String, Set<String>>> maps = new ArrayList<TreeMap<String, Set<String>>>();
	EndEnd ee = new EndEnd();
	BeginBegin bb = new BeginBegin();
	EndBegin eb = new EndBegin();
	// (BB1 & EE1) \ (EB0 U EB1)
	maps.add(bb.getPredecessorEvents(source, target, expression));
	maps.add(ee.getPredecessorEvents(source, target, expression));
	
	maps.add(eb.getConcurrentEvents(source, target, expression));
	maps.add(eb.getPredecessorEvents(source, target, expression));
	
	Mapping m = getMapping(maps);
	return m;
    }

    public double getRuntimeApproximation(int sourceSize, int targetSize, double theta, Language language) {
	return 1000d;
    }

    public double getMappingSizeApproximation(int sourceSize, int targetSize, double theta, Language language) {
	return 1000d;
    }
}
