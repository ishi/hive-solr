package com.chimpler.hive.solr;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;

public class SolrDocumentFactoryTest {

	@Test
	public void whenNoValuesProvided_shouldCreateEmptyDocument() throws Exception {
		// Given
		MapWritable map = new MapWritable();
		SolrDocumentFactory factory = new SolrDocumentFactory(false, new String[0]);
		// When
		SolrInputDocument document = factory.create(map);
		// Then
		assertThat(document.entrySet(), is(empty()));
	}

	@Test
	public void whenValuesProvided_shouldCreateCorrectDocument() throws Exception {
		// Given
		MapWritable map = new MapWritable();
		map.put(new Text("key"), new Text("value"));
		map.put(new Text("key-empty"), new Text(""));
		SolrDocumentFactory factory = new SolrDocumentFactory(false, new String[0]);
		// When
		SolrInputDocument document = factory.create(map);
		// Then
		assertThat(document.entrySet().size(), is(2));
		assertThat(document.get("key").getValue().toString(), is("value"));
		assertThat(document.get("key-empty").getValue().toString(), is(""));
	}

	@Test
	public void whenOmitAllEmpty_shouldCreateDocumentWithoutAllEmpties() throws Exception {
		// Given
		MapWritable map = new MapWritable();
		map.put(new Text("key"), new Text("value"));
		map.put(new Text("key-empty"), new Text(""));
		SolrDocumentFactory factory = new SolrDocumentFactory(true, new String[0]);
		// When
		SolrInputDocument document = factory.create(map);
		// Then
		assertThat(document.entrySet().size(), is(1));
		assertThat(document.containsKey("key"), is(true));
	}

	@Test
	public void whenOmitEmptyEnabledAndColumsProvided_shouldCreateDocumentWithoutSpecifiedEmpties() throws Exception {
		// Given
		MapWritable map = new MapWritable();
		map.put(new Text("key"), new Text("value"));
		map.put(new Text("key-empty"), new Text(""));
		map.put(new Text("key-empty2"), new Text(""));
		SolrDocumentFactory factory = new SolrDocumentFactory(true, new String[] {"key-empty"});
		// When
		SolrInputDocument document = factory.create(map);
		// Then
		assertThat(document.entrySet().size(), is(2));
		assertThat(document.containsKey("key"), is(true));
		assertThat(document.containsKey("key-empty2"), is(true));
	}
}
