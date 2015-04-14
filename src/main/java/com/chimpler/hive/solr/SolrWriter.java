package com.chimpler.hive.solr;

import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.io.*;

import java.io.IOException;

public class SolrWriter implements RecordWriter {
	private SolrTable table;
	private final SolrDocumentFactory documentFactory;

	public SolrWriter(SolrTable table, int numOutputBufferRows, SolrDocumentFactory documentFactory) {
		this.table = table;
		this.documentFactory = documentFactory;
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
		table.save( documentFactory.create(map));
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