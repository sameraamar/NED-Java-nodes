package com.ned.provider;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class Yieldery<T> {
	private LinkedBlockingQueue<T> stream;
	private AtomicInteger counter ;
	private boolean done;
	
	public Yieldery()
	{
		counter = new AtomicInteger(0);
		stream = new LinkedBlockingQueue<T>();
		done = false;
	}
	
	public void add(T t)
	{
		stream.add(t);
		counter.incrementAndGet();
	}
	
	public boolean isDone()
	{
		return done && stream.isEmpty();
	}	
	
	public void isDone(boolean done)
	{
		this.done = done;
	}
	
	public T poll()
	{
		return stream.poll();
	}
	
	public int getCounter()
	{
		return counter.get();
	}
}
