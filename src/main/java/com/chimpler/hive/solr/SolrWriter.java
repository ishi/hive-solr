package com.chimpler.hive.solr;

import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.io.*;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.util.Map;

public class SolrWriter implements RecordWriter {
	private SolrTable table;

	public SolrWriter(SolrTable table, int numOutputBufferRows) {
		this.table = table;
		if (numOutputBufferRows > 0) {
			table.setNumInputBufferRows(numOutputBufferRows);
		}

	}

	@Override
	public void close(boolean abort) throws IOException {
		if (!abort) {
			table.commit();
		} else {
			table.rollback();
		}
	}

	@Override
	public void write(Writable w) throws IOException {
		MapWritable map = (MapWritable) w;
		SolrInputDocument doc = new SolrInputDocument();
		for (final Map.Entry<Writable, Writable> entry : map.entrySet()) {
			String key = entry.getKey().toString();
			doc.setField(key, entry.getValue().toString());
		}
		table.save(doc);
	}

	private Object getObjectFromWritable(Writable w) {
		if (w instanceof IntWritable) {
			// int
			return ((IntWritable) w).get();
		} else if (w instanceof ShortWritable) {
			// short
			return ((ShortWritable) w).get();
		} else if (w instanceof ByteWritable) {
			// byte
			return ((ByteWritable) w).get();
		} else if (w instanceof BooleanWritable) {
			// boolean
			return ((BooleanWritable) w).get();
		} else if (w instanceof LongWritable) {
			// long
			return ((LongWritable) w).get();
		} else if (w instanceof FloatWritable) {
			// float
			return ((FloatWritable) w).get();
		} else if (w instanceof DoubleWritable) {
			// double
			return ((DoubleWritable) w).get();
		} else if (w instanceof NullWritable) {
			//null
			return null;
		} else {
			// treat as string
			return w.toString();
		}
	}
}