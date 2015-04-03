package com.chimpler.hive.solr;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

public class SolrTable {
	private static final int MAX_INPUT_BUFFER_ROWS = 100000;
	private static final int MAX_OUTPUT_BUFFER_ROWS = 100000;
	private SolrServer server;
	private Collection<SolrInputDocument> outputBuffer;
	private int numInputBufferRows = MAX_INPUT_BUFFER_ROWS;
	private int numOutputBufferRows = MAX_OUTPUT_BUFFER_ROWS;

	public SolrTable(SolrServer solrServer) {
		this.server = solrServer;
		this.outputBuffer = new ArrayList<SolrInputDocument>(numOutputBufferRows);
	}

	public void save(SolrInputDocument doc) throws IOException {
		outputBuffer.add(doc);

		if (outputBuffer.size() >= numOutputBufferRows) {
			flush();
		}
	}

	public void flush() throws IOException {
		try {
			if (!outputBuffer.isEmpty()) {
				server.add(outputBuffer);
				outputBuffer.clear();
			}
		} catch (SolrServerException e) {
			throw new IOException(e);
		}
	}

	public long count() throws IOException {
		return findAll(new String[0], 0, 0).getNumFound();
	}

	public SolrTableCursor findAll(String[] fields, int start, int count) throws IOException {
		return new SolrTableCursor(server, fields, start, count, numInputBufferRows);
	}

	public void drop() throws IOException {
		try {
			server.deleteByQuery("*:*");
			server.commit();
		} catch (SolrServerException e) {
			throw new IOException(e);
		}
	}

	public void commit() throws IOException {
		try {
			flush();
			server.commit();
		} catch (SolrServerException e) {
			throw new IOException(e.getMessage(), e);
		}
	}

	public void rollback() throws IOException {
		try {
			outputBuffer.clear();
			server.rollback();
		} catch (SolrServerException e) {
			throw new IOException(e.getMessage(), e);
		}
	}

	public int getNumOutputBufferRows() {
		return numOutputBufferRows;
	}

	public void setNumOutputBufferRows(int numOutputBufferRows) {
		this.numOutputBufferRows = numOutputBufferRows;
	}

	public int getNumInputBufferRows() {
		return numInputBufferRows;
	}

	public void setNumInputBufferRows(int numInputBufferRows) {
		this.numOutputBufferRows = numInputBufferRows;
	}

}