package com.chimpler.hive.solr;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.solr.common.SolrDocument;

import java.io.IOException;

public class SolrReader implements RecordReader<LongWritable, MapWritable> {
	private final static int BATCH_SIZE = 10000;
	private SolrSplit split;
	private SolrTableCursor cursor;
	private int pos;
	private int numBufferRows;

	String[] readColumns;

	public SolrReader(SolrTable table, SolrSplit split, String[] readColumns, int numInputBufferRows)
			throws IOException {
		this.split = split;
		this.numBufferRows = numBufferRows;

		this.readColumns = readColumns;
		if (numInputBufferRows > 0) {
			table.setNumInputBufferRows(numInputBufferRows);
		}
		cursor = table.findAll(readColumns, (int) split.getStart(), (int) split.getLength());
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public LongWritable createKey() {
		return new LongWritable();
	}

	@Override
	public MapWritable createValue() {
		return new MapWritable();
	}

	@Override
	public long getPos() throws IOException {
		return this.pos;
	}

	@Override
	public float getProgress() throws IOException {
		return split.getLength() > 0 ? pos / (float) split.getLength() : 1.0f;
	}

	@Override
	public boolean next(LongWritable keyHolder, MapWritable valueHolder)
			throws IOException {
		SolrDocument doc = cursor.nextDocument();
		if (doc == null) {
			return false;
		}

		keyHolder.set(pos++);

		for (int i = 0; i < readColumns.length; i++) {
			String key = readColumns[i];
			Object vObj = doc.getFieldValue(key);
			Writable value = (vObj == null) ? NullWritable.get() : new Text(vObj.toString());
			valueHolder.put(new Text(key), value);
		}
		return true;
	}

}
