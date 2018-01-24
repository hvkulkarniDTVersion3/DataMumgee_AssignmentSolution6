package com.stackroute.datamunger.reader;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import com.stackroute.datamunger.query.DataSet;
import com.stackroute.datamunger.query.DataTypeDefinitions;
import com.stackroute.datamunger.query.Filter;
import com.stackroute.datamunger.query.GroupDataSet;
import com.stackroute.datamunger.query.Header;
import com.stackroute.datamunger.query.Row;
import com.stackroute.datamunger.query.RowDataTypeDefinitions;
import com.stackroute.datamunger.query.parser.QueryParameter;
import com.stackroute.datamunger.query.parser.Restriction;

/* this is the CsvGroupByQueryProcessor class used for evaluating queries without 
 * aggregate functions but with group by clause*/
public class CsvGroupByQueryProcessor implements QueryProcessingEngine {
	RowDataTypeDefinitions rowDataTypeDefinitions = null;
	Header header = null;
	GroupDataSet groupDataSet = new GroupDataSet();

	public HashMap getResultSet(QueryParameter queryParameter) {
		DataSet dataSet = null;
		Long rowID = 0L;
		// initialize BufferedReader
		try {
			BufferedReader bufferedReader = new BufferedReader(
					new InputStreamReader(new FileInputStream(queryParameter.getFile())));
			dataSet = new DataSet();
			bufferedReader.mark(1);
			// read the first line which contains the header
			String headerRow = bufferedReader.readLine();

			// read the next line which contains the first row of data
			String firstRow = bufferedReader.readLine();

			// populate the header Map object from the header array
			header = new Header();
			String[] headers = headerRow.split(",");
			for (int index = 0; index < headers.length; index++) {
				header.put(headers[index], index);
			}
			// populate the dataType map object from the first line
			if (firstRow != null) {
				String[] firstRowParts = firstRow.split(",", -1);
				rowDataTypeDefinitions = new RowDataTypeDefinitions();
				int index = 0;
				for (String columnType : firstRowParts) {
					rowDataTypeDefinitions.put(index++, DataTypeDefinitions.getDataType(columnType).toString());
				}
			}

			// reset the buffered reader so that it can start reading from the
			// first line
			bufferedReader.reset();

			// skip the first line as it is already read earlier
			bufferedReader.readLine();
			// read one line at a time from the CSV file
			String currentRow = null;
			while ((currentRow = bufferedReader.readLine()) != null) {
				String[] rowValues = currentRow.split(",", -1);
				boolean isValidRow = true;

				// apply the conditions mentioned in the where clause on the row
				// data
				if (queryParameter.getRestrictions() != null) {
					isValidRow = false;
					List<String> logicalOperators = queryParameter.getLogicalOperators();
					boolean[] conditions = new boolean[queryParameter.getRestrictions().size()];
					int conditionsIndex = 0;
					for (Restriction restriction : queryParameter.getRestrictions()) {
						isValidRow = getRestrictedRecord(restriction, rowValues, rowDataTypeDefinitions);
						conditions[conditionsIndex++] = isValidRow;
					}
					/*
					 * check whether the selected record satisfies all the
					 * condition
					 */
					isValidRow = conditions[0];
					for (conditionsIndex = 1; conditionsIndex <= logicalOperators.size(); conditionsIndex++) {

						if (logicalOperators.get(conditionsIndex - 1).toLowerCase().trim().equals("or")) {
							isValidRow = isValidRow || conditions[conditionsIndex];
						} else {
							isValidRow = isValidRow && conditions[conditionsIndex];
						}
					}
				}
				if (isValidRow) {
					Row row = new Row();
					List<String> fields = queryParameter.getFields();
					Iterator<String> headerSet = header.keySet().iterator();

					while (headerSet.hasNext()) {
						String columnName = headerSet.next();
						if (fields.contains(columnName)) {							
							row.put(columnName, rowValues[header.get(columnName)]);
						}

						String groupByColumn = queryParameter.getGroupByFields().get(0);
						String groupByValue = rowValues[header.get(groupByColumn)];
						List<Row> groupRows = new ArrayList<>();
						if (groupDataSet.containsKey(groupByValue)) {
							groupRows = groupDataSet.get(groupByValue);
						}
						groupRows.add(row);
						groupDataSet.put(groupByValue, groupRows);
					}
				}
			}
		} catch (FileNotFoundException fileNotFoundException) {
			fileNotFoundException.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return groupDataSet;
	}

	boolean getRestrictedRecord(Restriction restriction, String columns[], RowDataTypeDefinitions typeDefinitions) {
		Filter filter = new Filter(typeDefinitions);
		boolean conditionFlag = false;
		if (restriction.getCondition().trim().equals("<")) {
			if (filter.isLTOperator(restriction.getPropertyValue(),
					columns[header.get(restriction.getPropertyName().trim())],
					header.get(restriction.getPropertyName().trim()))) {
				conditionFlag = true;
			} else {
				conditionFlag = false;
			}
		} else if (restriction.getCondition().trim().equals(">")) {
			if (filter.isGTOperator(restriction.getPropertyValue(),
					columns[header.get(restriction.getPropertyName().trim())],
					header.get(restriction.getPropertyName().trim()))) {
				conditionFlag = true;
			} else {
				conditionFlag = false;
			}
		} else if (restriction.getCondition().trim().equals("<=")) {
			if (filter.isLTEqualsOperator(restriction.getPropertyValue(),
					columns[header.get(restriction.getPropertyName().trim())],
					header.get(restriction.getPropertyName().trim()))) {
				conditionFlag = true;
			} else {
				conditionFlag = false;
			}
		} else if (restriction.getCondition().trim().equals(">=")) {
			if (filter.isGTEqualsOperator(restriction.getPropertyValue(),
					columns[header.get(restriction.getPropertyName().trim())],
					header.get(restriction.getPropertyName().trim()))) {
				conditionFlag = true;
			} else {
				conditionFlag = false;
			}
		}

		else if (restriction.getCondition().trim().equals("=")) {
			if (filter.isEqualsOperator(restriction.getPropertyValue(),
					columns[header.get(restriction.getPropertyName().trim())],
					header.get(restriction.getPropertyName().trim()))) {
				conditionFlag = true;
			} else {
				conditionFlag = false;
			}
		}

		else if (restriction.getCondition().trim().equals("!=")) {
			if (filter.isNotEqualsOperator(restriction.getPropertyValue(),
					columns[header.get(restriction.getPropertyName().trim())],
					header.get(restriction.getPropertyName().trim()))) {
				conditionFlag = true;
			} else {
				conditionFlag = false;
			}
		}

		return conditionFlag;
	}

	public DataSet groupData(Row row, String field) {
		DataSet dataSet = null;

		return dataSet;
	}

}