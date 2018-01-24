package com.stackroute.datamunger.query;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

//this class will be acting as the DataSet containing multiple rows
public class DataSet extends LinkedHashMap<Long, Row> {

	// this method will performing the sort on the dataset
	public DataSet sort(RowDataTypeDefinitions dataTypes, String columnName) {
		Header header = null;
		List<Row> rowsList = new ArrayList<>();
		Collections.sort(rowsList, new GenericComparator() {
			@Override
			public int compare(Row o1, Row o2) {
				int orderByColumnPosition = header.get(columnName);
				String columnDatatype = dataTypes.get(columnName);
				if (columnDatatype.equals("java.lang.Integer")) {
					return Integer.parseInt(o1.get(columnName)) - Integer.parseInt(o2.get(columnName));
				} else if (columnDatatype.equals("java.lang.String")) {
					return o1.get(columnName).compareTo(o2.get(columnName));
				} else if (columnDatatype.equals("java.util.Date")) {
					try {
						return new SimpleDateFormat("dd/MM/yyyy").parse(o1.get(columnName))
								.compareTo(new SimpleDateFormat("dd/MM/yyyy").parse(o2.get(columnName)));
					} catch (ParseException e) {
						e.printStackTrace();
					}
				}
				return 0;
			}

		});
		DataSet sortedDataSet=new DataSet();
		long rowId=0L;
		for(Row row:rowsList){
			sortedDataSet.put(rowId++, row);
		}
		return sortedDataSet;
	}
}	