package com.ned.provider;

public abstract class ActivityBase<T> extends Thread 
{
	private Publisher<T> publish;
	private Yieldery<T> bus;
	private boolean isdone;
	
	public ActivityBase()
	{
	}
	
	public Yieldery<T> init()
	{
		isdone = false;
		this.bus = new Yieldery<T>();
		initHook();
		return this.bus;
	}
	

	protected void safeSleep(int ms) {
		try {
			sleep(ms);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public Yieldery<T> getBus()
	{
		return this.bus;
	}
	
	public boolean isDone()
	{
		return isdone;
	}
	
	public void run()
	{
		doRun();
		
		getBus().isDone(true);
		System.out.println(this.getClass().getSimpleName() + ": done.");
		isdone = true;
	}
	
	abstract protected void doRun();
	abstract protected void initHook();
	
}
