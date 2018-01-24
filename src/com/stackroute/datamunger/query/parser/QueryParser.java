package com.stackroute.datamunger.query.parser;

import java.util.ArrayList;
import java.util.List;

public class QueryParser {
	private final QueryParameter queryParameter = new QueryParameter();

	// this method will parse the queryString and will return the object of
	// QueryParameter
	// class
	public QueryParameter parseQuery(String queryString) {
		queryParameter.setFile(getFile(queryString));
		queryParameter.setBaseQuery(getBaseQuery(queryString));
		queryParameter.setOrderByFields(getOrderByFields(queryString));
		queryParameter.setGroupByFields(getGroupByFields(queryString));
		queryParameter.setFields(getFields(queryString));
		queryParameter.setRestrictions(getConditions(queryString));
		queryParameter.setLogicalOperators(getLogicalOperators(queryString));
		queryParameter.setAggregateFunctions(getAggregateFunctions(queryString));
		queryParameter.setQUERY_TYPE(getQUERY_TYPE(queryString));
		return queryParameter;
	}

	public String getBaseQuery(String queryString) {
		String words[] = queryString.toLowerCase().split(" ");
		String baseQuery = "";
		for (String word : words) {
			if (word.equals("where") || word.equals("group by") || word.equals("order by")) {
				break;
			}
			baseQuery += word + " ";
		}
		baseQuery=baseQuery.trim();
		return baseQuery;
	}

	public String getFile(String queryString) {

		String fileName = "";
		if (queryString.contains(" from ")) {

			fileName = queryString.split(" from ")[1];
			// change logic
			fileName = fileName.split(".csv")[0] + ".csv";
		}
		return fileName;
	}

	/*
	 * extract the order by fields from the query string. Please note that we
	 * will need to extract the field(s) after "order by" clause in the query,
	 * if at all the order by clause exists. For eg: select
	 * city,winner,team1,team2 from data/ipl.csv order by city from the query
	 * mentioned above, we need to extract "city". Please note that we can have
	 * more than one order by fields.
	 */
	public List<String> getOrderByFields(String queryString) {
		List<String> orderByFields = new ArrayList<String>();
		if (queryString.contains(" order by ")) {
			String orderByCol = queryString.split(" order by ")[1];
			String[] orderByColList = orderByCol.split(",");
			for (String str : orderByColList) {
				orderByFields.add(str.trim());
			}
			return orderByFields;
		} else {
			return null;
		}
	}

	/*
	 * extract the group by fields from the query string. Please note that we
	 * will need to extract the field(s) after "group by" clause in the query,
	 * if at all the group by clause exists. For eg: select
	 * city,max(win_by_runs) from data/ipl.csv group by city from the query
	 * mentioned above, we need to extract "city". Please note that we can have
	 * more than one group by fields.
	 */
	public List<String> getGroupByFields(String queryString) {
		List<String> groupByFields = new ArrayList<String>();
		if (queryString.contains(" group by ")) {
			queryString = queryString.contains(" order By ") ? queryString.trim().split(" order ")[0].trim()
					: queryString;
			/*
			 * if (queryString.contains(" order by ")) {
			 */
			queryString = queryString.trim().split(" order ")[0].trim();

			String groupByCol = queryString.trim().split("group by")[1].trim();
			String[] groupByColList = groupByCol.split(",");
			for (String groupbyColList : groupByColList) {
				groupByFields.add(groupbyColList);
			}
		}
		return groupByFields;
	}
	/*
	 * extract the selected fields from the query string. Please note that we
	 * will need to extract the field(s) after "select" clause followed by a
	 * space from the query string. For eg: select city,win_by_runs from
	 * data/ipl.csv from the query mentioned above, we need to extract "city"
	 * and "win_by_runs". Please note that we might have a field containing name
	 * "from_date" or "from_hrs". Hence, consider this while parsing.
	 */

	public List<String> getFields(String queryString) {
		List<String> colList = new ArrayList<String>();
		String selectColumn = queryString.split("select")[1].trim();
		String selectColumnList = selectColumn.split("from")[0].trim();
		if (selectColumnList.trim().contains("*") || selectColumnList.trim().length() == 1) {
			colList.add("*");
		} else {
			String collist[] = selectColumnList.split(",");

			for (String colist : collist) {
				colList.add(colist.trim());
			}
		}
		return colList;
	}

	/*
	 * extract the conditions from the query string(if exists). for each
	 * condition, we need to capture the following: 1. Name of field 2.
	 * condition 3. value
	 * 
	 * For eg: select city,winner,team1,team2,player_of_match from data/ipl.csv
	 * where season >= 2008 or toss_decision != bat
	 * 
	 * here, for the first condition, "season>=2008" we need to capture: 1. Name
	 * of field: season 2. condition: >= 3. value: 2008
	 * 
	 * the query might contain multiple conditions separated by OR/AND
	 * operators. Please consider this while parsing the conditions.
	 * 
	 */
	/*
	 * public String getConditionsPartQuery(String queryString) {
	 * System.out.println("in getConditionsPartQuery"); if
	 * (queryString.contains("where")) { String conditionPart =
	 * queryString.split("where")[1].trim(); conditionPart =
	 * conditionPart.split("group|order")[0].trim();
	 * System.out.println(conditionPart); return conditionPart; } else { return
	 * null; } }
	 */

	public List<Restriction> getConditions(String queryString) {
		List<Restriction> queryParameterComponents = new ArrayList<Restriction>();
		// String conditionComponents = getConditionsPartQuery(queryString);
		if (queryString.contains("where")) {
			String conditionComponents = queryString.split(" where ")[1];
			if (conditionComponents.contains(" group by ") || conditionComponents.contains(" order by ")) {

				conditionComponents = conditionComponents.split("group|order")[0];
			}
			String[] conditions = null;
			if (conditionComponents != null) {
				conditions = conditionComponents.trim().split("\\s+and\\s+|\\s+or\\s");
			}
			for (String condition : conditions) {
				Restriction restriction = new Restriction();
				String propertyname = condition.trim().split("\\s*[>=|<=|!=|=|>|<]")[0].trim();
				// String propertyvalue =
				// condition.trim().split("\\s*[>=|<=|!=|=|>|<]")[1].trim();
				/*
				 * String operator =
				 * condition.trim().substring(condition.indexOf(propertyname) +
				 * propertyname.length(),
				 * condition.indexOf(propertyvalue)).trim();
				 */
				String operator = null;
				if (condition.contains("<=")) {
					operator = "<=";
				} else if (condition.contains("<")) {
					operator = "<";
				} else if (condition.contains(">=")) {
					operator = ">=";
				} else if (condition.contains(">")) {
					operator = ">";
				} else if (condition.contains("!=")) {
					operator = "!=";
				} else if (condition.contains("=")) {
					operator = "=";
				}
				String propertyvalue = condition.trim().split(operator)[1].trim();
				propertyvalue.trim();
				if(propertyvalue.contains("\'")){
					propertyvalue=propertyvalue.substring(1,propertyvalue.length()-1);
				}
				restriction.setCondition(operator);
				restriction.setPropertyName(propertyname);
				restriction.setPropertyValue(propertyvalue);
				queryParameterComponents.add(restriction);
			}
			return queryParameterComponents;
		} else {
			return null;
		}
	}

	/*
	 * extract the logical operators(AND/OR) from the query, if at all it is
	 * present. For eg: select city,winner,team1,team2,player_of_match from
	 * data/ipl.csv where season >= 2008 or toss_decision != bat and city =
	 * bangalore
	 * 
	 * the query mentioned above in the example should return a List of Strings
	 * containing [or,and]
	 */
	public List<String> getLogicalOperators(String queryString) {
		List<String> logiOperator = new ArrayList<String>();
		if (queryString.contains(" where ")) {
			String conditions = queryString.split(" where ")[1].trim();
			conditions = conditions.split(" group | order ")[0].trim();
			if (conditions != null) {
				String[] logicalOperators = conditions.split("\\s+");
				for (String logiop : logicalOperators) {
					if (logiop.equals("and".toLowerCase()) || logiop.equals("or".toLowerCase())) {
						logiOperator.add(logiop);
					}
				}
			}
			return logiOperator;
		} else {
			return null;
		}
	}

	/*
	 * extract the aggregate functions from the query. The presence of the
	 * aggregate functions can determined if we have either "min" or "max" or
	 * "sum" or "count" or "avg" followed by opening braces"(" after "select"
	 * clause in the query string. in case it is present, then we will have to
	 * extract the same. For each aggregate functions, we need to know the
	 * following: 1. type of aggregate function(min/max/count/sum/avg) 2. field
	 * on which the aggregate function is being applied
	 * 
	 * Please note that more than one aggregate function can be present in a
	 * query
	 * 
	 * 
	 */
	public List<AggregateFunction> getAggregateFunctions(String queryString) {
		List<String> colList = getFields(queryString);
		List<AggregateFunction> aggregateList = new ArrayList<AggregateFunction>();
		for (String collist : colList) {
			{
				if (collist.startsWith("max(") || collist.startsWith("min(") || collist.startsWith("sum(")
						|| collist.startsWith("count(") || collist.startsWith("avg(")) {
					AggregateFunction aggregateFunction = new AggregateFunction();
					String[] aggrfunction = collist.split("\\(");
					aggregateFunction.setField(aggrfunction[1].split("\\)")[0]);
					aggregateFunction.setFunction(aggrfunction[0]);
					aggregateList.add(aggregateFunction);
				}
			}
		}
		return aggregateList;
	}

	private String getQUERY_TYPE(String queryString) {
		String QUERY_TYPE = "BasicQuery";
		if (!queryParameter.getAggregateFunctions().isEmpty() && !queryParameter.getGroupByFields().isEmpty()) {
			QUERY_TYPE = "AggregateGroupByQuery";
		} else if (!queryParameter.getAggregateFunctions().isEmpty()) {
			QUERY_TYPE = "AggregateQuery";
		} else if (!queryParameter.getGroupByFields().isEmpty()) {
			QUERY_TYPE = "GroupByQuery";
		}
		System.out.println("Query type is " + QUERY_TYPE);
		return QUERY_TYPE;
	}
}