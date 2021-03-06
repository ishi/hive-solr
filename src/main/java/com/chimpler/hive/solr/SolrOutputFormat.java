package com.chimpler.hive.solr;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.Properties;

public class SolrOutputFormat implements OutputFormat<NullWritable, Row>,
		HiveOutputFormat<NullWritable, Row> {

	private SolrServerFactory serverFactory = new SolrServerFactory();

	@Override
	public RecordWriter getHiveRecordWriter(JobConf conf,
			Path finalOutPath,
			Class<? extends Writable> valueClass,
			boolean isCompressed,
			Properties tableProperties,
			Progressable progress) throws IOException {
		return new SolrWriter(
				new SolrTable(serverFactory.getInstance(conf)),
				ConfigurationUtil.getNumOutputBufferRows(conf),
				new SolrDocumentFactory(
						ConfigurationUtil.shouldOmitEmpty(conf),
						ConfigurationUtil.getOmitEmptyColumns(conf))
		);

	}

	@Override
	public void checkOutputSpecs(FileSystem arg0, JobConf conf)
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public org.apache.hadoop.mapred.RecordWriter<NullWritable, Row> getRecordWriter(
			FileSystem arg0, JobConf arg1, String arg2, Progressable arg3)
			throws IOException {
		throw new RuntimeException("Error: Hive should not invoke this method.");
	}

}
