package com.boot.welcome;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.json.CDL;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.google.api.client.json.Json;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import com.google.gson.Gson;

@RestController
@RequestMapping("/cloud")
public class BigQueryDataRetrieval {

	@RequestMapping("/welcome")
	public String sayWelcome() {		
		return "Welcome to Google Cloud Platforms with Spring Boot Application";
	}

	@PostMapping("/add/{id}/{name}/{email}")
	public String addCustomerInfo(@PathVariable("id") long id, @PathVariable("name") String name, @PathVariable("email") String email) throws FileNotFoundException, IOException, InterruptedException {
		
		System.out.println("Welcome to BIG QUERY - GCP CONSOLE Update...");

		long start = System.currentTimeMillis();
		BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId("black-resource-249308").setCredentials(ServiceAccountCredentials.
				fromStream(new FileInputStream("C:\\GCP\\My First Project-d53cc6829172.json"))).build().getService();
		
		String sqlQuery = "INSERT INTO `black-resource-249308.MyFirstBigQuery_DataSet.EMP_INFO` VALUES (" + id + ", '" + name + "', '" + email + "')";
					
		QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sqlQuery).setUseLegacySql(false).build();

		// Create a job ID so that we can safely retry.
		JobId jobId = JobId.of(UUID.randomUUID().toString());
		com.google.cloud.bigquery.Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

		// Wait for the query to complete.
		queryJob = queryJob.waitFor();
		
		TableResult result = queryJob.getQueryResults();		
		System.out.println("Total Number of Records :: " + result.getTotalRows());
		
		long end = System.currentTimeMillis();
	      //finding the time difference and converting it into seconds
	    float sec = (end - start) / 1000F; 
	    System.out.println("Total time taken to process : " + sec + " seconds");//9.226 seconds for 10000 records
	    
	    return "Successfully added the record into Google Cloud Platform based on the input";
	}
	
	@DeleteMapping("/delete/{empId}")
	public String deleteCustomerInfo(@PathVariable("empId") String empId) throws FileNotFoundException, IOException, InterruptedException {
		
		System.out.println("Welcome to BIG QUERY - GCP CONSOLE Delete...");

		long start = System.currentTimeMillis();
		BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId("black-resource-249308").setCredentials(ServiceAccountCredentials.
				fromStream(new FileInputStream("C:\\GCP\\My First Project-d53cc6829172.json"))).build().getService();
		
		String sqlQuery = "DELETE FROM `black-resource-249308.MyFirstBigQuery_DataSet.EMP_INFO` WHERE EMP_ID =" + empId;
					
		QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sqlQuery).setUseLegacySql(false).build();

		// Create a job ID so that we can safely retry.
		JobId jobId = JobId.of(UUID.randomUUID().toString());
		com.google.cloud.bigquery.Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

		// Wait for the query to complete.
		queryJob = queryJob.waitFor();
		
		TableResult result = queryJob.getQueryResults();		
		System.out.println("Total Number of Records after deleting the current record :: " + result.getTotalRows());
		
		long end = System.currentTimeMillis();
	      //finding the time difference and converting it into seconds
	    float sec = (end - start) / 1000F; 
	    System.out.println("Total time taken to process : " + sec + " seconds");//9.226 seconds for 10000 records
	    
		return "Successfully deleted the record from Google Cloud Platform based on the input";
	}

	
	@PutMapping("/update/{id}/{name}")
	public String updateCustomerInfo(@PathVariable("id") long id, @PathVariable("name") String name) throws FileNotFoundException, IOException, InterruptedException {
		
		System.out.println("Welcome to BIG QUERY - GCP CONSOLE Update...");

		long start = System.currentTimeMillis();
		BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId("black-resource-249308").setCredentials(ServiceAccountCredentials.
				fromStream(new FileInputStream("C:\\GCP\\My First Project-d53cc6829172.json"))).build().getService();
		
		String sqlQuery = "UPDATE `black-resource-249308.MyFirstBigQuery_DataSet.EMP_INFO` SET EMP_NAME = " + "'" + name + "'" + " WHERE EMP_ID = " + id;
					
		QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sqlQuery).setUseLegacySql(false).build();

			// Create a job ID so that we can safely retry.
			JobId jobId = JobId.of(UUID.randomUUID().toString());
			com.google.cloud.bigquery.Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

			// Wait for the query to complete.
			queryJob = queryJob.waitFor();
			
			TableResult result = queryJob.getQueryResults();		
			System.out.println("Total Number of Records after update :: " + result.getTotalRows());
			
			long end = System.currentTimeMillis();
		      //finding the time difference and converting it into seconds
		    float sec = (end - start) / 1000F; 
		    System.out.println(sec + " seconds");//9.226 seconds for 10000 records
		    
		    return "Successfully udpated the record into Google Cloud Platform based on the input";
	}
	
	
	@RequestMapping("/retrieve")
	public String retrieveCustomerInfo() throws FileNotFoundException, IOException, InterruptedException {
		
		System.out.println("Welcome to BIG QUERY - GCP CONSOLE ");
		//BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
		long start = System.currentTimeMillis();
		BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId("black-resource-249308")
				.setCredentials(ServiceAccountCredentials.
				fromStream(new FileInputStream("C:\\GCP\\My First Project-d53cc6829172.json"))).build().getService();
		
		//String sqlQuery = "SELECT * FROM `black-resource-249308.MyFirstBigQuery_DataSet.EMP_INFO`";
		String sqlQuery = "SELECT Emp_ID, FirstName, EMail FROM `black-resource-249308.MyFirstBigQuery_DataSet.CUSTOMER_INFO`";
					
		QueryJobConfiguration queryConfig = QueryJobConfiguration
					.newBuilder(sqlQuery)
			        .setUseLegacySql(false)
			        .build();

			// Create a job ID so that we can safely retry.
			JobId jobId = JobId.of(UUID.randomUUID().toString());
			com.google.cloud.bigquery.Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

			// Wait for the query to complete.
			queryJob = queryJob.waitFor();

			// Check for errors
			if (queryJob == null) {
			  throw new RuntimeException("Job no longer exists");
			} else if (queryJob.getStatus().getError() != null) {
			  // You can also look at queryJob.getStatus().getExecutionErrors() for all
			  // errors, not just the latest one.
			  throw new RuntimeException(queryJob.getStatus().getError().toString());
			}
			
			// Get the results.
			TableResult result = queryJob.getQueryResults();
			
			List<Employee> employeeList = new ArrayList<>();

			// Print all pages of the results.
			for (FieldValueList row : result.iterateAll()) {
			  
				//EMP_INFO Table
			  /*long empId = row.get("EMP_ID").getLongValue();
			  String name = row.get("EMP_NAME").getStringValue();
			  System.out.printf("Name : %s Employee Id : %d%n", name, empId);*/
				
				Employee employee = new Employee();
				//CUSOTMER_INFO Table 
				  long empId = row.get("Emp_ID").getLongValue();
				  employee.setEmpId(empId);
				  String name = row.get("FirstName").getStringValue();
				  employee.setFirstName(name);
				  String email = row.get("EMail").getStringValue(); 
				  employee.setEmail(email);
				  //System.out.printf("Employee Id : %d Name : %s Email : %s %n", empId, name, email);
				  employeeList.add(employee);				
			}
			
			Gson gson = new Gson();
			String jsonOutput = gson.toJson(employeeList);
			//System.out.println(jsonOutput);
			
			//JSONObject jsonObject = new JSONObject(employeeList);
			//System.out.println(jsonObject);
			JSONArray jsonArray = new JSONArray(employeeList);
			//System.out.println(jsonArray);
			convertJSONArrayObjectToCsvFile(jsonArray);
			
			//convert to flat file
			convertJSONArrayObjectToFlatFile(jsonArray);
		    
			long end = System.currentTimeMillis();
		      //finding the time difference and converting it into seconds
		    float sec = (end - start) / 1000F; 
		    System.out.println(sec + " seconds");//9.226 seconds for 10000 records
		
		return "Welcome to Google Cloud Platforms with Spring Boot Application";
	}
	
	private void convertJSONArrayObjectToFlatFile(JSONArray jsonArray)  {
		FileWriter file = null;
		try {
			file = new FileWriter("C:\\GCP\\Sample_CSV.ods");
			for (int i = 0; i < jsonArray.length(); i++) {
				JSONObject jsonObject = jsonArray.getJSONObject(i);
				String lineStr = jsonObject.getLong("empId") + " " + jsonObject.getString("firstName") + " " + jsonObject.getString("email") + " ";
				file.write(lineStr);
				file.write("\r\n");
			}
			file.flush();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (file != null) {
				try {
					file.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
	private void convertJSONArrayObjectToCsvFile(JSONArray jsonArray) {
		File file = new File("C:\\GCP\\FlatFile.txt");
		String csvStr = CDL.toString(jsonArray);
		try {
			FileUtils.writeStringToFile(file, csvStr);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	@RequestMapping("/csvProcess")
	public String csvProcess() {
		
		//String str = "[{"empId":177173,"firstName":"Usha","email":"usha.gile@btinternet.com"},{"empId":493558,"firstName":"Pierre","email":"pierre.peoples@gmail.com"},{"empId":198897,"firstName":"Daisy","email":"daisy.mccalla@earthlink.net"}]";
		
		return "Welcome to Google Cloud Platforms with Spring Boot Application";
	}
	
	//String str = [{"empId":177173,"firstName":"Usha","email":"usha.gile@btinternet.com"},{"empId":493558,"firstName":"Pierre","email":"pierre.peoples@gmail.com"},{"empId":198897,"firstName":"Daisy","email":"daisy.mccalla@earthlink.net"}]
}

//https://cloud.google.com/bigquery/docs/quickstarts/quickstart-client-libraries#client-libraries-install-java
//https://cloud.google.com/eclipse/docs/libraries
//SELECT * FROM `black-resource-249308.MyFirstBigQuery_DataSet.EMP_INFO` LIMIT 1000



/*String tempSQl = "\"SELECT \"\r\n" + 
"			          + \"CONCAT('https://stackoverflow.com/questions/', CAST(id as STRING)) as url, \"\r\n" + 
"			          + \"view_count \"\r\n" + 
"			          + \"FROM `bigquery-public-data.stackoverflow.posts_questions` \"\r\n" + 
"			          + \"WHERE tags like '%google-bigquery%' \"\r\n" + 
"			          + \"ORDER BY favorite_count DESC LIMIT 10\";
*/

//INSERT INTO `black-resource-249308.MyFirstBigQuery_DataSet.EMP_INFO` VALUES(1003, 'Paul', 'test3@gmail.com');


//API KEY --> AIzaSyADXJq3_p7aWMn3TFNlg40LK95xECrEyeI
//GOOGLE_APPLICATION_CREDENTIALS=C:\GCP\My First Project-d53cc6829172.json

//https://github.com/googleapis/google-cloud-java/blob/master/google-cloud-examples/src/main/java/com/google/cloud/examples/bigquery/BigQueryExample.java