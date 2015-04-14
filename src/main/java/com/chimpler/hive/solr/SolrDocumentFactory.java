package com.chimpler.hive.solr;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.solr.common.SolrInputDocument;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;

/**
 * Created by Radoslaw_Zielinski on 4/9/2015.
 */
public class SolrDocumentFactory {
	private final boolean shouldOmmitEmpty;
	private final HashSet<String> columnsToOmmitWhenEmpty = new HashSet<>();

	public SolrDocumentFactory(boolean shouldOmmitEmpty, String[] columnsToOmmit) {
		this.shouldOmmitEmpty = shouldOmmitEmpty;
		if(columnsToOmmit.length > 0) {
			Collections.addAll(this.columnsToOmmitWhenEmpty, columnsToOmmit);
		}
	}

	public SolrInputDocument create(MapWritable map) {
		SolrInputDocument doc = new SolrInputDocument();
		for (final Map.Entry<Writable, Writable> entry : map.entrySet()) {
			String key = entry.getKey().toString();
			String value = entry.getValue().toString();
			if(shouldOmmitEmpty && "".equals(value)) {
				if(columnsToOmmitWhenEmpty.isEmpty()) {
					continue;
				}
				if(columnsToOmmitWhenEmpty.contains(key)) {
					continue;
				}
			}
			doc.setField(key, value);
		}
		return doc;
	}
}
