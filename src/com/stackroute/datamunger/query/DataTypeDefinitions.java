package com.stackroute.datamunger.query;

//this class contains methods to find the column data types
public class DataTypeDefinitions {

	public static Object getDataType(String input) {
		if (input.matches("^\\d+$")) {
			return new Integer(input).getClass().getName();
		} else if (input.matches("^\\d*\\.\\d*$")) {
			return new Double(input).getClass().getName();
		} else if (input.matches("^\\d{1,2}\\/\\d{1,2}\\/\\d{4}$")
				|| input.matches("^\\d{1,2}\\-\\[A-Za-z]{3}\\-\\d{2}$")
				|| input.matches("^\\d{1,2}\\-\\[A-Za-z]{3}\\-\\d{4}$")
				|| input.matches("^\\d{1,2}\\-\\[A-Za-z]{5}\\-\\d{2}$")
				|| input.matches("^\\d{1,2}\\-\\[A-Za-z]{5}\\-\\d{4}$")
				|| input.matches("^\\d{4}\\-\\d{1,2}\\-\\d{1,2}$")) {
			return new java.util.Date().getClass().getName();
		} else if (input.matches("^\\w[a-zA-Z_0-9]*.*$")) {
			return new String().getClass().getName();
		} else {
			return new Object();
		}
		// check for empty object

		// checking for Integer

		// checking for floating point numbers

		// checking for date format dd/mm/yyyy

		// checking for date format mm/dd/yyyy

		// checking for date format dd-mon-yy

		// checking for date format dd-mon-yyyy

		// checking for date format dd-month-yy

		// checking for date format dd-month-yyyy
	}
}