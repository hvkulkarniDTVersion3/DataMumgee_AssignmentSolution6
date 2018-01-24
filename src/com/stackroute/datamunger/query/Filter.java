package com.stackroute.datamunger.query;

import java.util.Date;

//this class contains methods to evaluate expressions
public class Filter {
	RowDataTypeDefinitions rowDataTypeDefinitions;

	public Filter(RowDataTypeDefinitions rowDataTypeDefinitions) {
		this.rowDataTypeDefinitions = rowDataTypeDefinitions;
	}
	// method to perform sum operation

	// method to evaluate expression for eg: salary>20000

	// method containing implementation of equalTo operator
	public boolean isEqualsOperator(String conditionValue, String columnValue, int columnIndex) {
		if (rowDataTypeDefinitions.get(columnIndex).contains("Integer")) {
			return Integer.parseInt(conditionValue) == Integer.parseInt(columnValue);
		} else if (rowDataTypeDefinitions.get(columnIndex).contains("Double")) {
			return Double.parseDouble(conditionValue) == Double.parseDouble(columnValue);
		} else if (rowDataTypeDefinitions.get(columnIndex).contains("Date")) {
			return new Date(conditionValue) == (new Date(columnValue));
		} else {
			return conditionValue.compareToIgnoreCase(columnValue) == 0;
		}
	}

	public boolean isNotEqualsOperator(String conditionValue, String columnValue, int columnIndex) {
		if (rowDataTypeDefinitions.get(columnIndex).contains("Integer")) {
			return Integer.parseInt(conditionValue) != Integer.parseInt(columnValue);
		} else if (rowDataTypeDefinitions.get(columnIndex).contains("Double")) {
			return Double.parseDouble(conditionValue) != Double.parseDouble(columnValue);
		} else if (rowDataTypeDefinitions.get(columnIndex).contains("Date")) {
			return new Date(conditionValue) != (new Date(columnValue));
		} else {
			return (!conditionValue.equals(columnValue));
		}
	}

	// method containing implementation of greaterThan operator
	public boolean isGTOperator(String conditionValue, String columnValue, int columnIndex) {
		if (rowDataTypeDefinitions.get(columnIndex).contains("Integer")) {
			return Integer.parseInt(columnValue) > Integer.parseInt(conditionValue);
		} else if (rowDataTypeDefinitions.get(columnIndex).contains("Double")) {
			return Double.parseDouble(columnValue) > Double.parseDouble(conditionValue);
		} else if (rowDataTypeDefinitions.get(columnIndex).contains("Date")) {
			return new Date(columnValue).after(new Date(conditionValue));
		} else {
			return columnValue.compareToIgnoreCase(conditionValue) == 1;
		}
	}

	// method containing implementation of greaterThanOrEqualTo operator
	public boolean isGTEqualsOperator(String conditionValue, String columnValue, int columnIndex) {
		if (rowDataTypeDefinitions.get(columnIndex).contains("Integer")) {
			return Integer.parseInt(columnValue) >= Integer.parseInt(conditionValue);
		} else if (rowDataTypeDefinitions.get(columnIndex).contains("Double")) {
			return Double.parseDouble(conditionValue) >= Double.parseDouble(conditionValue);
		} else if (rowDataTypeDefinitions.get(columnIndex).contains("Date")) {
			return new Date(conditionValue).after(new Date(conditionValue))
					|| new Date(conditionValue).compareTo(new Date(conditionValue)) == 0;
		} else {
			return conditionValue.compareToIgnoreCase(conditionValue) == 1;
		}
	}

	// method containing implementation of lessThan operator
	public boolean isLTOperator(String conditionValue, String columnValue, int columnIndex) {
		if (rowDataTypeDefinitions.get(columnIndex).contains("Integer")) {
			return Integer.parseInt(columnValue) < Integer.parseInt(conditionValue);
		} else if (rowDataTypeDefinitions.get(columnIndex).contains("Double")) {
			return Double.parseDouble(columnValue) < Double.parseDouble(conditionValue);
		} else if (rowDataTypeDefinitions.get(columnIndex).contains("Date")) {
			return new Date(columnValue).before(new Date(conditionValue));
		} else {
			return columnValue.compareToIgnoreCase(conditionValue) == -1;
		}
	}

	// method containing implementation of lessThanOrEqualTo operator
	public boolean isLTEqualsOperator(String conditionValue, String columnValue, int columnIndex) {
		if (rowDataTypeDefinitions.get(columnIndex).contains("Integer")) {
			return Integer.parseInt(columnValue) <= Integer.parseInt(conditionValue);
		} else if (rowDataTypeDefinitions.get(columnIndex).contains("Double")) {
			return Double.parseDouble(columnValue) <= Double.parseDouble(conditionValue);
		} else if (rowDataTypeDefinitions.get(columnIndex).contains("Date")) {
			return new Date(columnValue).before(new Date(conditionValue))
					|| new Date(columnValue).compareTo(new Date(conditionValue)) == 0;

		} else {
			return columnValue.compareToIgnoreCase(conditionValue) == -1;
		}
	}
}
