package com.stackroute.datamunger.reader;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.stackroute.datamunger.query.DataSet;
import com.stackroute.datamunger.query.DataTypeDefinitions;
import com.stackroute.datamunger.query.Filter;
import com.stackroute.datamunger.query.GroupDataSet;
import com.stackroute.datamunger.query.Header;
import com.stackroute.datamunger.query.Row;
import com.stackroute.datamunger.query.RowDataTypeDefinitions;
import com.stackroute.datamunger.query.parser.AggregateFunction;
import com.stackroute.datamunger.query.parser.QueryParameter;
import com.stackroute.datamunger.query.parser.Restriction;

/* this is the CsvGroupByAggregateQueryProcessor class used for evaluating queries with 
 * aggregate functions and group by clause*/
public class CsvGroupByAggregateQueryProcessor implements QueryProcessingEngine {
	RowDataTypeDefinitions rowDataTypeDefinitions = null;
	Header header = null;

	public HashMap getResultSet(QueryParameter queryParameter) {

		// initialize BufferedReader
		DataSet dataSet = null;
		Long rowID = 0L;
		GroupDataSet groupDataSet = new GroupDataSet();
		List<AggregateFunction> aggregateFunctions = null;
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

				Iterator<String> headerSet = header.keySet().iterator();
				List<Restriction> restrictions = queryParameter.getRestrictions();
				// apply the conditions mentioned in the where clause on the
				// row
				// data
				boolean isValidRow = true;
				if (restrictions != null) {
					isValidRow = false;
					List<String> logicalOperators = queryParameter.getLogicalOperators();
					/*
					 * check for multiple conditions in where clause for eg:
					 * where salary>20000 and city=Bangalore for eg: where
					 * salary>20000 or city=Bangalore and dept!=Sales
					 */

					/* evaluate multiple conditions */
					boolean[] conditions = new boolean[queryParameter.getRestrictions().size()];
					int conditionsIndex = 0;
					for (Restriction restriction : restrictions) {
						isValidRow = getRestrictedRecord(restriction, rowValues, rowDataTypeDefinitions);
						conditions[conditionsIndex++] = isValidRow;
					}
					/* check if the row is selected */
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
					// checking if the group by column value is there in the
					// map
					aggregateFunctions = queryParameter.getAggregateFunctions();
					for (AggregateFunction aggregateFunction : aggregateFunctions) {
						String columnName = aggregateFunction.getField();
						if (columnName.equals("*")) {
							columnName = (new ArrayList(header.keySet())).get(0).toString();
							row.put(columnName, rowValues[header.get(columnName)]);
						}
						// Evaluate the aggregate functions with group by
						// clause
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
				Set<String> groupSetKeys = groupDataSet.keySet();
				Iterator<String> groupSetKeysIterator = groupSetKeys.iterator();
				while (groupSetKeysIterator.hasNext()) {
					List<AggregateFunction> aggregateFunctionsList = aggregateFunctions;
					String groupValue = groupSetKeysIterator.next();
					List<Row> groupRecords = groupDataSet.get(groupValue);
					int index = 0;
					for (Row groupRecord : groupRecords) {
						for (int funIndex = 0; funIndex < aggregateFunctions.size(); funIndex++) {
							AggregateFunction aggregateFunction = aggregateFunctions.get(funIndex);
							String field = aggregateFunction.getField();
							if (field.equals("*")) {
								field = (new ArrayList(header.keySet())).get(0).toString();
							}
							String rowValue = groupRecord.get(field);
							String columnType = rowDataTypeDefinitions.get(header.get(field));
							if (aggregateFunction.getFunction().equals("sum")) {
								if (columnType.equals("java.lang.Integer") || columnType.equals("java.lang.Double")) {
									aggregateFunction.setSum(aggregateFunction.getSum()
											+ Double.parseDouble(rowValues[header.get(field)]));
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
											date = new SimpleDateFormat("dd/mm/yyyy")
													.parse(rowValues[header.get(field)]);
										} else {
											date = new SimpleDateFormat("dd/mm/yyyy")
													.parse(aggregateFunction.getResult());
										}
										if (new SimpleDateFormat("dd/mm/yyyy").parse(rowValues[header.get(field)])
												.after(date)) {
											date = new SimpleDateFormat("dd/mm/yyyy")
													.parse(rowValues[header.get(field)]);
											aggregateFunction
													.setResult(new SimpleDateFormat("dd/mm/yyyy").format(date));
										}
									}
								} else if (aggregateFunction.getFunction().equals("min")) {
									if (columnType.equals("java.lang.Integer")|| columnType.equals("java.lang.Double")) {
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
											date = new SimpleDateFormat("dd/mm/yyyy")
													.parse(rowValues[header.get(field)]);
										} else {
											date = new SimpleDateFormat("dd/mm/yyyy")
													.parse(aggregateFunction.getResult());
										}
										if (new SimpleDateFormat("dd/mm/yyyy").parse(rowValues[header.get(field)])
												.after(date)) {
											date = new SimpleDateFormat("dd/mm/yyyy")
													.parse(rowValues[header.get(field)]);
											aggregateFunction
													.setResult(new SimpleDateFormat("dd/mm/yyyy").format(date));
										}
									}
								}
							} else if (aggregateFunction.getFunction().equals("avg")) {
								if (columnType.equals("java.lang.Integer") || columnType.equals("java.lang.Double")) {
									aggregateFunction.setSum(aggregateFunction.getSum()
											+ Double.parseDouble(rowValues[header.get(field)]));
									aggregateFunction.setCount(aggregateFunction.getCount() + 1);
									aggregateFunction.setResult(
											Double.toString(aggregateFunction.getSum() / aggregateFunction.getCount()));
								}
							}
							aggregateFunctions.set(funIndex, aggregateFunction);
						}
						index++;
					}
					Row row = new Row();
					for (int position = 0, aggregateFieldPosition = 0; position < queryParameter.getFields()
							.size(); position++) {
						if (queryParameter.getFields().get(position).contains("(")) {
							row.put(queryParameter.getFields().get(position),
									aggregateFunctions.get(aggregateFieldPosition++).getResult());
						} else {
							row.put(queryParameter.getFields().get(position), groupValue);
						}
					}
					dataSet.put(rowID++, row);
				}
			}
		}
		// add the row object to the dataset object
		catch (FileNotFoundException fileNotFoundException) {
			fileNotFoundException.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
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
