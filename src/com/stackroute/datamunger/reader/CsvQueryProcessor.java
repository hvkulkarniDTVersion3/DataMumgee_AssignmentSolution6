package com.stackroute.datamunger.reader;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Set;

import com.stackroute.datamunger.query.DataSet;
import com.stackroute.datamunger.query.DataTypeDefinitions;
import com.stackroute.datamunger.query.Filter;
import com.stackroute.datamunger.query.Header;
import com.stackroute.datamunger.query.Row;
import com.stackroute.datamunger.query.RowDataTypeDefinitions;
import com.stackroute.datamunger.query.parser.QueryParameter;
import com.stackroute.datamunger.query.parser.Restriction;

//this class will read from CSV file and process and return the resultSet
public class CsvQueryProcessor implements QueryProcessingEngine {
	RowDataTypeDefinitions rowDataTypeDefinitions = null;
	Header header = null;

	public DataSet getResultSet(QueryParameter queryParameter) {

		// initialize BufferedReader
		DataSet dataSet = null;
		Long rowID = 0L;
		try {
			@SuppressWarnings("resource")
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

			// read one line at a time from the CSV file
			bufferedReader.readLine();
			String currentRow = null;
			while ((currentRow = bufferedReader.readLine()) != null) {
				String[] rowValues = currentRow.split(",", -1);
				boolean isValidRow = true;
				if (queryParameter.getRestrictions() != null) {
					isValidRow = false;
					List<String> logicalOperators = queryParameter.getLogicalOperators();
					boolean[] conditions = new boolean[queryParameter.getRestrictions().size()];
					int conditionsIndex = 0;
					for (Restriction restriction : queryParameter.getRestrictions()) {
						isValidRow = getRestrictedRecord(restriction, rowValues, rowDataTypeDefinitions);
						conditions[conditionsIndex++] = isValidRow;
					}
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
					if (queryParameter.getBaseQuery().contains("*")) {
						Set<String> colNames = header.keySet();
						for (String colName : colNames) {
							row.put(colName, rowValues[header.get(colName)]);
						}

					} else {
						for (String colName : queryParameter.getFields()) {
							row.put(colName, rowValues[header.get(colName)]);
						}
					}
					dataSet.put(rowID++, row);
				}

				// apply the conditions mentioned in the where clause on the row
				// data

				// check if the row read satisfies all the conditions

				// check if all columns are required

				// get the selected columns to be selected and push it to row
				// object

				// add the row object to the dataset object
			}
		} catch (FileNotFoundException fileNotFoundException) {
			fileNotFoundException.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} /*
			 * catch (ParseException e) { e.printStackTrace(); }
			 */
		// return dataset
		return dataSet;
	}

	public boolean getRestrictedRecord(Restriction restriction, String columns[],
			RowDataTypeDefinitions rowDataTypeDefinitions) {
		Filter filter = new Filter(rowDataTypeDefinitions);
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
}