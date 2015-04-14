package com.chimpler.hive.solr;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class ConfigurationUtilTest {

	@Test
	public void whenNullIsGivenAsColumns_shouldReturnEmptyArray() {
		// Given
		String columnsString = null;
		// When
		String[] columns = ConfigurationUtil.getAllColumns(columnsString);
		// Then
		assertThat(columns, is(notNullValue()));
		assertThat(columns.length, is(0));
	}

	@Test
	public void whenEmptyStringIsGivenAsColumns_shouldReturnEmptyArray() {
		// Given
		String columnsString = "";
		// When
		String[] columns = ConfigurationUtil.getAllColumns(columnsString);
		// Then
		assertThat(columns, is(notNullValue()));
		assertThat(columns.length, is(0));
	}

	@Test
	public void whenOneColumnIsGiven_shouldReturnOneElementArray() {
		// Given
		String columnsString = "column1";
		// When
		String[] columns = ConfigurationUtil.getAllColumns(columnsString);
		// Then
		assertThat(columns, is(notNullValue()));
		assertThat(columns.length, is(1));
	}

	@Test
	public void whenManyColumnsIsGiven_shouldReturnManyElementsInArray() {
		// Given
		String columnsString = "column1,column2 , column3";
		// When
		String[] columns = ConfigurationUtil.getAllColumns(columnsString);
		// Then
		assertThat(columns, is(notNullValue()));
		assertThat(columns.length, is(3));
	}

	@Test
	public void whenNoOmitEmptyParameters_shouldOmitEmptyShouldReturnFalse() {
		// Given
		Configuration configuration = new Configuration();
		// When
		boolean shouldOmmitEmpty = ConfigurationUtil.shouldOmitEmpty(configuration);
		// Then
		assertThat(shouldOmmitEmpty, is(false));
	}

	@Test
	public void whenShouldOmmitIsTrue_shouldOmitEmptyShouldReturnTrue() {
		// Given
		Configuration configuration = new Configuration();
		configuration.set(ConfigurationUtil.OMIT_EMPTY, "true");
		// When
		boolean shouldOmmitEmpty = ConfigurationUtil.shouldOmitEmpty(configuration);
		// Then
		assertThat(shouldOmmitEmpty, is(true));
	}

	@Test
	public void whenShouldOmmitIsFalse_shouldOmitEmptyShouldReturnFalse() {
		// Given
		Configuration configuration = new Configuration();
		configuration.set(ConfigurationUtil.OMIT_EMPTY, "false");
		// When
		boolean shouldOmmitEmpty = ConfigurationUtil.shouldOmitEmpty(configuration);
		// Then
		assertThat(shouldOmmitEmpty, is(false));
	}

	@Test
	public void whenShouldOmmitColumnsIsGiven_shouldOmitEmptyShouldReturnTrue() {
		// Given
		Configuration configuration = new Configuration();
		configuration.set(ConfigurationUtil.OMIT_EMPTY_COLUMNS, "column1");
		// When
		boolean shouldOmmitEmpty = ConfigurationUtil.shouldOmitEmpty(configuration);
		// Then
		assertThat(shouldOmmitEmpty, is(true));
	}

	@Test
	public void whenShouldOmmitColumnsIsNotGiven_shouldReturnEmptyArray() {
		// Given
		Configuration configuration = new Configuration();
		// When
		String[] columns = ConfigurationUtil.getOmitEmptyColumns(configuration);
		// Then
		assertThat(columns.length, is(0));
	}

	@Test
	public void whenShouldOmmitColumnsIsGiven_shouldReturnNonEmptyArray() {
		// Given
		Configuration configuration = new Configuration();
		configuration.set(ConfigurationUtil.OMIT_EMPTY_COLUMNS, "column1");
		// When
		String[] columns = ConfigurationUtil.getOmitEmptyColumns(configuration);
		// Then
		assertThat(columns.length, is(not(0)));
	}

}