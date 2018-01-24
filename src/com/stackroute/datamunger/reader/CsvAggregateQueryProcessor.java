package com.stackroute.datamunger.reader;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import com.stackroute.datamunger.query.DataSet;
import com.stackroute.datamunger.query.DataTypeDefinitions;
import com.stackroute.datamunger.query.Filter;
import com.stackroute.datamunger.query.Header;
import com.stackroute.datamunger.query.Row;
import com.stackroute.datamunger.query.RowDataTypeDefinitions;
import com.stackroute.datamunger.query.parser.AggregateFunction;
import com.stackroute.datamunger.query.parser.QueryParameter;
import com.stackroute.datamunger.query.parser.Restriction;

/* this is the CsvAggregateQueryProcessor class used for evaluating queries with 
 * aggregate functions without group by clause*/
public class CsvAggregateQueryProcessor implements QueryProcessingEngine {
	RowDataTypeDefinitions rowDataTypeDefinitions = null;
	Header header = null;

	public HashMap getResultSet(QueryParameter queryParameter) {
		DataSet dataSet = null;
		Long rowID = 0L;
		try {
			BufferedReader bufferedReader = new BufferedReader(
					new InputStreamReader(new FileInputStream(queryParameter.getFile())));
			dataSet = new DataSet();
			bufferedReader.mark(1);
			// initialize BufferedReader
			String headerRow = bufferedReader.readLine();

			// read the first line which contains the header

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
			int index = 0;
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
				// apply the conditions mentioned in the where clause on the row
				// data
				/*
				 * check for multiple conditions in where clause for eg: where
				 * salary>20000 and city=Bangalore for eg: where salary>20000 or
				 * city=Bangalore and dept!=Sales
				 */

				/*
				 * if the row is selected after evaluating all conditions
				 */
				/* evaluate all the aggregate functions one by one */
				Row row = new Row();
				List<AggregateFunction> functionList = queryParameter.getAggregateFunctions();
				if (isValidRow) {
					for (int funIndex = 0; funIndex < functionList.size(); funIndex++) {
						AggregateFunction aggregateFunction = functionList.get(funIndex);
						String field = aggregateFunction.getField();
						String columnType = rowDataTypeDefinitions.get(header.get(field));
						if (aggregateFunction.getFunction().equals("sum")) {
							if (columnType.equals("java.lang.Integer") || columnType.equals("java.lang.Double")) {
								aggregateFunction.setSum(
										aggregateFunction.getSum() + Double.parseDouble(rowValues[header.get(field)]));
							}
							aggregateFunction.setResult(Double.toString(aggregateFunction.getSum()));
						} else if (aggregateFunction.getFunction().equals("count")) {
							if (!columnType.equals("java.lang.Object")) {
								aggregateFunction.setCount(aggregateFunction.getCount() + 1);
							}
							aggregateFunction.setResult(Integer.toString(aggregateFunction.getCount()));
						} else if (aggregateFunction.getFunction().equals("max")) {
							if (columnType.equals("java.lang.Integer") || columnType.equals("java.lang.Double")) {
								double maxValue;
								if (index == 0) {
									maxValue = Double.parseDouble(rowValues[header.get(field)]);
								} else {
									maxValue = Double.parseDouble(aggregateFunction.getResult());
								}
								if (Double.parseDouble(rowValues[header.get(field)]) > maxValue) {
									maxValue = Double.parseDouble(rowValues[header.get(field)]);
									aggregateFunction.setResult(Double.toString(maxValue));
								} else if (columnType.equals("java.util.Date")) {
									Date date = null;
									if (index == 0) {
										date = new SimpleDateFormat("dd/mm/yyyy").parse(rowValues[header.get(field)]);
									} else {
										date = new SimpleDateFormat("dd/mm/yyyy").parse(aggregateFunction.getResult());
									}
									if (new SimpleDateFormat("dd/mm/yyyy").parse(rowValues[header.get(field)])
											.after(date)) {
										date = new SimpleDateFormat("dd/mm/yyyy").parse(rowValues[header.get(field)]);
										aggregateFunction.setResult(new SimpleDateFormat("dd/mm/yyyy").format(date));
									}
								}
							} else if (aggregateFunction.getFunction().equals("min")) {
								if (columnType.equals("java.lang.Integer") || columnType.equals("java.lang.Double")) {
									double minValue;
									if (index == 0) {
										minValue = Double.parseDouble(rowValues[header.get(field)]);
									} else {
										minValue = Double.parseDouble(aggregateFunction.getResult());
									}
									if (Double.parseDouble(rowValues[header.get(field)]) > minValue) {
										minValue = Double.parseDouble(rowValues[header.get(field)]);
										aggregateFunction.setResult(Double.toString(minValue));
									}
								} else if (columnType.equals("java.util.Date")) {
									Date date = null;
									if (index == 0) {
										date = new SimpleDateFormat("dd/mm/yyyy").parse(rowValues[header.get(field)]);
									} else {
										date = new SimpleDateFormat("dd/mm/yyyy").parse(aggregateFunction.getResult());
									}
									if (new SimpleDateFormat("dd/mm/yyyy").parse(rowValues[header.get(field)])
											.after(date)) {
										date = new SimpleDateFormat("dd/mm/yyyy").parse(rowValues[header.get(field)]);
										aggregateFunction.setResult(new SimpleDateFormat("dd/mm/yyyy").format(date));
									}
								}
							}
						} else if (aggregateFunction.getFunction().equals("avg")) {
							if (columnType.equals("java.lang.Integer") || columnType.equals("java.lang.Double")) {
								aggregateFunction.setSum(
										aggregateFunction.getSum() + Double.parseDouble(rowValues[header.get(field)]));
								aggregateFunction.setCount(aggregateFunction.getCount() + 1);
								aggregateFunction.setResult(
										Double.toString(aggregateFunction.getSum() / aggregateFunction.getCount()));
							}
						}
						functionList.set(funIndex, aggregateFunction);
					}
					index++;
				}
				for (int position = 0; position < queryParameter.getFields().size(); position++) {
					row.put(queryParameter.getFields().get(position), functionList.get(position).getResult());
				}
				dataSet.put(rowID, row);
			}
		} catch (FileNotFoundException fileNotFoundException) {
			fileNotFoundException.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return dataSet;
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
}