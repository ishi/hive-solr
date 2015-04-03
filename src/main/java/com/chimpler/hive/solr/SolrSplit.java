package com.chimpler.hive.solr;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SolrSplit extends FileSplit implements InputSplit {
	private static final String[] EMPTY_ARRAY = new String[] { };
	private long start, end;
	private boolean isLastSplit = false;

	public SolrSplit() {
		super((Path) null, 0, 0, EMPTY_ARRAY);
	}

	public SolrSplit(long start, long end, Path dummyPath) {
		super(dummyPath, 0, 0, EMPTY_ARRAY);
		this.start = start;
		this.end = end;
	}

	@Override
	public void readFields(final DataInput input) throws IOException {
		super.readFields(input);
		start = input.readLong();
		end = input.readLong();
	}

	@Override
	public void write(final DataOutput output) throws IOException {
		super.write(output);
		output.writeLong(start);
		output.writeLong(end);
	}

	public long getStart() {
		return start;
	}

	public long getEnd() {
		return end;
	}

	public boolean isLastSplit() {
		return this.isLastSplit;
	}

	public void setStart(long start) {
		this.start = start;
	}

	public void setEnd(long end) {
		this.end = end;
	}

	public void setLastSplit() {
		this.isLastSplit = true;
	}

	@Override
	public long getLength() {
		return end - start;
	}

	/* Data is remote for all nodes. */
	@Override
	public String[] getLocations() throws IOException {
		return EMPTY_ARRAY;
	}

	@Override
	public String toString() {
		return String.format("MongoSplit(start=%s,end=%s)", start, end);
	}

	public static SolrSplit[] getSplits(JobConf conf,
			SolrTable table, int numSplits) throws IOException {

		long total = table.count();
		int _numSplits = (numSplits < 1 || total <= numSplits) ? 1 : numSplits;
		final long splitSize = total / _numSplits;
		SolrSplit[] splits = new SolrSplit[_numSplits];
		final Path[] tablePaths = FileInputFormat.getInputPaths(conf);
		for (int i = 0; i < _numSplits; i++) {
			if ((i + 1) == _numSplits) {
				splits[i] = new SolrSplit(i * splitSize, total, tablePaths[0]);
				splits[i].setLastSplit();
			} else {
				splits[i] = new SolrSplit(i * splitSize, (i + 1) * splitSize, tablePaths[0]);
			}
		}

		return splits;
	}
}