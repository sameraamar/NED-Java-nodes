package com.ned.main;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.ned.provider.DocumentParserActivity;
import com.ned.provider.FileGZipReaderActivity;
import com.ned.provider.LSHActivity;
import com.ned.types.Tupel2;
import com.ned.types.temp.GlobalData;
import com.ned.util.Utility;

public class TestMain {

	public static void main(String[] args) throws Exception {
		GlobalData.getInstance().init();
		
		FileGZipReaderActivity reader = new FileGZipReaderActivity();
		reader.init();
		reader.setDaemon(true);
		
		DocumentParserActivity docActivity = new DocumentParserActivity( reader.getBus() );
		docActivity.init();
		docActivity.setDaemon(true);
		
		LSHActivity lsh = new LSHActivity( docActivity.getBus() );
		lsh.init();
		lsh.setDaemon(true);
		
		lsh.start();
		docActivity.start();
		reader.start();

		int counter = 0;
		boolean flag = true;
		
		if(counter < 1000000) 
			sleep();

		
		long base = System.nanoTime();
		
		while(!lsh.isDone())
		{
			counter = lsh.getBus().getCounter();
			Tupel2<String, List<String>> val = lsh.getBus().poll();
			
			while(val == null)
			{
				sleep();
			}
			if(counter % 10000 == 0)
			{
				long currenttime = System.nanoTime();
				long seconds = TimeUnit.NANOSECONDS.toSeconds( currenttime - base );
				double seconds_avg = 1.0 * ( currenttime - base ) / counter;
				
				String msg = String.format("Counter: %d - Elapsed [%s] AHT: [%s], %d, %f", 
													counter, 
													Utility.humanTime(seconds), 
													Utility.humanTime((long)seconds_avg), 
													(long)seconds_avg,
													1.0*TimeUnit.NANOSECONDS.toSeconds((long)seconds_avg));
				System.out.println(msg);
			}
			
		}
		
		System.out.println("done.");
	}

	private static void sleep() {
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
