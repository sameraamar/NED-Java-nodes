package com.ned.provider;

import com.ned.types.Document;
import com.ned.types.Tupel2;

public class DocumentParserActivity extends ActivityBase<Tupel2<Document, Integer>> 
{
	Yieldery<String> lines;
	
	public DocumentParserActivity(Yieldery<String> lines)
	{
		this.lines = lines;
	}

	@Override
	protected void doRun() 
	{
		int counter= 0;
		while(!lines.isDone())
		{
			String json = lines.poll();
			while(json == null)
			{
				safeSleep(10);
				json = lines.poll();
			}
			Document doc = Document.parse(json, true);
			counter++;
			getBus().add(new Tupel2<Document, Integer>( doc, counter));
			
			if(counter % 10000 == 0)
			{
				System.out.println(this.getClass().getSimpleName() + ": " + counter);
			}
		}
		

	}

	@Override
	protected void initHook() 
	{
		
	}

}
