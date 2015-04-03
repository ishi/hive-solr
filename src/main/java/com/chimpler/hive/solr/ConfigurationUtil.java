package com.chimpler.hive.solr;

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class ConfigurationUtil {
	public static final String URL = "solr.url";
	public static final String ZK_URL = "solr.zk.url";
	public static final String ZK_COLLECTION = "solr.zk.collection";
	public static final String COLUMN_MAPPING = "solr.column.mapping";
	public static final String BUFFER_IN_ROWS = "solr.buffer.input.rows";
	public static final String BUFFER_OUT_ROWS = "solr.buffer.output.rows";

	public static final Set<String> ALL_PROPERTIES = ImmutableSet.of(URL, ZK_URL, ZK_COLLECTION, COLUMN_MAPPING,
			BUFFER_IN_ROWS, BUFFER_OUT_ROWS);

	public final static String getColumnMapping(Configuration conf) {
		return conf.get(COLUMN_MAPPING);
	}

	public final static String getUrl(Configuration conf) {
		return conf.get(URL);
	}

	public static boolean isZkUrlProvided(JobConf conf) {
		return getZkUrl(conf) != null;
	}

	public static String getZkUrl(JobConf conf) {
		return conf.get(ZK_URL);
	}

	public static String getZkCollection(JobConf conf) {
		return conf.get(ZK_COLLECTION);
	}

	public final static int getNumInputBufferRows(Configuration conf) {
		String value = conf.get(BUFFER_IN_ROWS, "-1");
		return Integer.parseInt(value);
	}

	public final static int getNumOutputBufferRows(Configuration conf) {
		String value = conf.get(BUFFER_OUT_ROWS, "-1");
		return Integer.parseInt(value);
	}

	public static void copySolrProperties(Properties from, JobConf to) {
		for (String key : ALL_PROPERTIES) {
			String value = from.getProperty(key);
			if (value != null) {
				to.set(key, value);
			}
		}
	}

	public static void copySolrProperties(Properties from,
			Map<String, String> to) {
		for (String key : ALL_PROPERTIES) {
			String value = from.getProperty(key);
			if (value != null) {
				to.put(key, value);
			}
		}
	}

	public static String[] getAllColumns(String columnMappingString) {
		String[] columns = columnMappingString.split(",");

		String[] trimmedColumns = new String[columns.length];
		for (int i = 0; i < columns.length; i++) {
			trimmedColumns[i] = columns[i].trim();
		}
		return trimmedColumns;
	}
}