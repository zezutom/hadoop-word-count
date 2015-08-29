package org.zezutom.hadoop.wordcount;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class WordCount extends Configured implements Tool {

	private static final Logger LOGGER = org.slf4j.LoggerFactory.getLogger(WordCount.class);
	
	// MAPPER CLASS
	static class MapperImpl extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);

		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			StringTokenizer it = new StringTokenizer(line);
			
			while (it.hasMoreTokens()) {
				word.set(it.nextToken());
				output.collect(word, one);
			}
		}

	}

	// REDUCER CLASS
	static class ReducerImpl extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
				Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}

	static int printUsage(String error) {
		LOGGER.error(error);
		LOGGER.info("wordcount [-m #mappers ] [-r #reducers] input_file output_file");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	enum ArgType {
		MAP, REDUCE, OTHER
	}

	@Override
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), WordCount.class);
		conf.setJobName("wordcount");
		
		// Text (words) as keys
		conf.setOutputKeyClass(Text.class);

		// Integers (counts) as values
		conf.setOutputValueClass(IntWritable.class);

		// Map -> (Sort -> Shuffle) -> Reduce
		conf.setMapperClass(MapperImpl.class);
		conf.setCombinerClass(ReducerImpl.class);
		conf.setReducerClass(ReducerImpl.class);

		List<String> otherArgs = new ArrayList<>();
		Iterator<String> it = Arrays.asList(args).iterator();

		while (it.hasNext()) {
			String arg = it.next();
			ArgType type;
			switch (arg) {
			case "-m":
				type = ArgType.MAP;
				break;
			case "-r":
				type = ArgType.REDUCE;
				break;
			default:
				type = ArgType.OTHER;
			}

			switch (type) {
			case MAP:
			case REDUCE:
				String argValue = null;
				try {
					argValue = it.next();
					int value = Integer.parseInt(argValue);
					if (type == ArgType.MAP) {
						conf.setNumMapTasks(value);
					} else {
						conf.setNumReduceTasks(value);
					}
				} catch (NoSuchElementException e) {
					return printUsage("Required parameter missing from " + arg);
				} catch (NumberFormatException e) {
					return printUsage("Integer expectd instead of " + argValue);
				}
				break;
			default:
				otherArgs.add(arg);
			}
		}

		// There must be exactly 2 additional parameters
		if (otherArgs.size() != 2) {
			return printUsage("Wrong number of parameters: " + otherArgs.size() + " instead of 2.");
		}
		FileInputFormat.setInputPaths(conf, new Path(otherArgs.get(0)));
		FileOutputFormat.setOutputPath(conf, new Path(otherArgs.get(1)));

		JobClient.runJob(conf);

		return 0;
	}

	static class WordCountRunner {
		private static String[] args;
		
		static void init(String[] args) {
			WordCountRunner.args = args;
		}
		
		public WordCountRunner() throws Exception {
			Configuration conf = new Configuration();
			conf.addResource(new Path(conf("core-site.xml")));
			conf.addResource(new Path(conf("hdfs-site.xml")));
			
			ToolRunner.run(conf, new WordCount(), args);
		}
		
		private String conf(String file) {
			return Paths.get(System.getenv("HADOOP_CONF_DIR"), file).toAbsolutePath().toString();
		}
	}
	public static void main(String[] args) throws Exception {	
		WordCountRunner.init(args);
		SpringApplication.run(WordCountRunner.class, args);
		//int status = ToolRunner.run(new Configuration(), new WordCount(), args);
		//System.exit(status);
	}
}
