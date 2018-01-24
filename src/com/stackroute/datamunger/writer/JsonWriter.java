package com.stackroute.datamunger.writer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class JsonWriter {
	
	public boolean writeToJson(Map resultSet) {
		
		BufferedWriter writer=null;
		Gson gson=new GsonBuilder().setPrettyPrinting().create();
		String result=gson.toJson(resultSet);
		
		try {
			writer=new BufferedWriter(new FileWriter("data/result.json"));
			writer.write(result);
			return true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("File writing failed");
			
			return false;
		}
		finally {
			try {
				writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}	
		
	}

}