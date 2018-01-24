package com.stackroute.datamunger;

import java.util.Scanner;

import com.stackroute.datamunger.query.Query;
import com.stackroute.datamunger.writer.JsonWriter;

public class DataMunger {

	public static void main(String[] args) {
		System.out.println("Enter a query ");
		String queryString = new Scanner(System.in).nextLine();
		// read the query from the user
		Query query = new Query();
		// instantiate Query class
		JsonWriter writer = new JsonWriter();

		// call executeQuery() method to get the resultSet and write to JSON
		// file

		if (writer.writeToJson(query.executeQuery(queryString))) {
			System.out.println("Output written to resources/result.json");
		}
	}
}