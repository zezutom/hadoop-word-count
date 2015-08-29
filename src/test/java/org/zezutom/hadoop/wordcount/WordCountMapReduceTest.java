package org.zezutom.hadoop.wordcount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.MapReduceDriver;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class WordCountMapReduceTest {

	// Words are delimited by spaces and semicolons
	public static final String TEXT = "655209;1;796764372490213 804422938115889 6;1 6 1";

	private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;

	private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;

	private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

	@Before
	public void setUp() {
		WordCountMapper mapper = new WordCountMapper();
		WordCountReducer reducer = new WordCountReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}

	@Test
	public void mapper() throws IOException {
		List<Pair<Text, IntWritable>> expectedOutput = new ArrayList<>();
		for (String word : new String[] { "1", "1", "1", "6", "6", "655209", "796764372490213", "804422938115889" }) {
			expectedOutput.add(new Pair<Text, IntWritable>(new Text(word), new IntWritable(1)));
		}
		mapDriver.withInput(new LongWritable(), new Text(TEXT));
		mapDriver.withAllOutput(expectedOutput);
		mapDriver.runTest();
	}

	@Test
	public void reducer() throws IOException {
		List<IntWritable> values = new ArrayList<IntWritable>();
		final int total = 3;
		for (int i = 0; i < total; i++) {
			values.add(new IntWritable(1));
		}
	    reduceDriver.withInput(new Text("6"), values);
	    reduceDriver.withOutput(new Text("6"), new IntWritable(total));
	    reduceDriver.runTest();		
	}

	@Test
	public void mapReduce() throws IOException {
		List<Pair<Text, IntWritable>> expectedOutput = new ArrayList<>();
		expectedOutput.add(wordCount("1", 3));
		expectedOutput.add(wordCount("6", 2));
		expectedOutput.add(wordCount("655209", 1));
		expectedOutput.add(wordCount("796764372490213", 1));
		expectedOutput.add(wordCount("804422938115889", 1));
				
		mapReduceDriver.withInput(new LongWritable(), new Text(TEXT));
	    mapReduceDriver.withAllOutput(expectedOutput);
	    mapReduceDriver.runTest();		
	}
	
	private Pair<Text, IntWritable> wordCount(String word, int count) {
		return new Pair<Text, IntWritable>(new Text(word), new IntWritable(count));		
	}

}
