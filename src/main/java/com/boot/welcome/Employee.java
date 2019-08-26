package com.boot.welcome;

public class Employee {

	private long empId;
	private String firstName;
	private String email;
	
	public Employee() {
		
	}	
	
	public Employee(long empId, String firstName, String email) {
		super();
		this.empId = empId;
		this.firstName = firstName;
		this.email = email;
	}
	public long getEmpId() {
		return empId;
	}
	public void setEmpId(long empId) {
		this.empId = empId;
	}
	public String getFirstName() {
		return firstName;
	}
	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}
	public String getEmail() {
		return email;
	}
	public void setEmail(String email) {
		this.email = email;
	}
	
	
	
}
