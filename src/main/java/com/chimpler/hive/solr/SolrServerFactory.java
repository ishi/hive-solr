package com.chimpler.hive.solr;

import org.apache.hadoop.mapred.JobConf;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;

import java.net.MalformedURLException;

/**
 * Created by Radoslaw_Zielinski on 4/3/2015.
 */
public class SolrServerFactory {
	public static SolrServer getInstance(JobConf conf) throws MalformedURLException {
		if (ConfigurationUtil.isZkUrlProvided(conf)) {
			CloudSolrServer solrServer = new CloudSolrServer(ConfigurationUtil.getZkUrl(conf));
			solrServer.setDefaultCollection(ConfigurationUtil.getZkCollection(conf));
			return solrServer;
		}
		return new HttpSolrServer(ConfigurationUtil.getUrl(conf));
	}
}
