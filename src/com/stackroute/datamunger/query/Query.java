package com.stackroute.datamunger.query;

import java.util.HashMap;

import com.stackroute.datamunger.query.parser.QueryParameter;
import com.stackroute.datamunger.query.parser.QueryParser;
import com.stackroute.datamunger.reader.CsvAggregateQueryProcessor;
import com.stackroute.datamunger.reader.CsvGroupByAggregateQueryProcessor;
import com.stackroute.datamunger.reader.CsvGroupByQueryProcessor;
import com.stackroute.datamunger.reader.CsvQueryProcessor;
import com.sun.corba.se.impl.orbutil.RepositoryIdUtility;

public class Query {

	public HashMap executeQuery(String queryString) {
		QueryParameter queryParameter = new QueryParser().parseQuery(queryString);
		// checking type of Query
		if (queryParameter.getQUERY_TYPE().equals("AggregateGroupByQuery")) {
			CsvGroupByAggregateQueryProcessor csvGroupByAggregateQueryProcessor = new CsvGroupByAggregateQueryProcessor();
			return csvGroupByAggregateQueryProcessor.getResultSet(queryParameter);
		} else if (queryParameter.getQUERY_TYPE().equals("AggregateQuery")) {
			CsvAggregateQueryProcessor csvAggregateQueryProcessor = new CsvAggregateQueryProcessor();
			System.out.println("Calling 			CsvAggregateQueryProcessor");
			return csvAggregateQueryProcessor.getResultSet(queryParameter);
		} else if (queryParameter.getQUERY_TYPE().equals("GroupByQuery")) {
			return new CsvGroupByQueryProcessor().getResultSet(queryParameter);
		} else if (queryParameter.getQUERY_TYPE().equals("BasicQuery")) {
			CsvQueryProcessor csvQueryProcessor = new CsvQueryProcessor();
			return csvQueryProcessor.getResultSet(queryParameter);
		}
		// queries without aggregate functions, order by clause or group by
		// clause

		// queries with aggregate functions

		// Queries with group by clause

		// Queries with group by and aggregate functions

		return null;
	}
}