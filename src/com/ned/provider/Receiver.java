package com.ned.provider;

public class Receiver<T> 
{
	int counter;
	
	public void doSomething(T t) 
	{
		counter++;
		System.out.println("received : " + counter + " of type " + t.getClass().getSimpleName());
	}
	
}
