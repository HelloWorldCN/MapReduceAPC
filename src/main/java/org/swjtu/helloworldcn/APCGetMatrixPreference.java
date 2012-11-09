/**
 * 
 */
package org.swjtu.helloworldcn;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.HadoopUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Tang 
 * @since 2012-03
 * Compute the preference value from input data
 * 从数据中获取偏度值
 */
public final class APCGetMatrixPreference {
	private static final Logger log = LoggerFactory
			.getLogger(APCGetMatrixPreference.class);
	

	public static double runJob(Path affInput,Path output)
			throws IOException, ClassNotFoundException, InterruptedException {

		// set up all the job tasks
		Configuration conf = new Configuration();
		Path diagOutput = new Path(output, "preference"+ (System.nanoTime() & 0xFF));
		HadoopUtil.delete(conf, diagOutput);
		Job job = new Job(conf, "APCGetMatrixPreference");

		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setMapperClass(APCGetMatrixPreferenceMapper.class);
		job.setReducerClass(APCGetMatrixPreferenceReducer.class);

		FileInputFormat.addInputPath(job, affInput);
		FileOutputFormat.setOutputPath(job, diagOutput);

		job.setJarByClass(APCGetMatrixPreference.class);

		job.waitForCompletion(true);
		SequenceFile.Reader reader= new SequenceFile.Reader(FileSystem.get(conf),new Path(diagOutput, "part-r-00000"),
				conf);
		DoubleWritable doubleWritable=new DoubleWritable();
		reader.next(NullWritable.get(), doubleWritable);
		
		// read the results back from the path
		return doubleWritable.get();
	}

	public static class APCGetMatrixPreferenceMapper
			extends
			Mapper<LongWritable, Text, NullWritable, DoubleWritable> {
		private static final Pattern COMMA_PATTERN = Pattern.compile(",");

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] elements = COMMA_PATTERN.split(value.toString());
			log.debug("(DEBUG - MAP) Key[{}], Value[{}]", key.get(), value);
			// store the sum
		
			context.write(NullWritable.get(), new DoubleWritable(Double.valueOf(elements[2])));
		}
	}

	public static class APCGetMatrixPreferenceReducer
			extends
			Reducer<NullWritable, DoubleWritable, NullWritable, DoubleWritable> {
		private int type=0; //How to compute preference
		private double getPreference(Iterable<DoubleWritable> values){
			int nums=0;
			double preference=0.0;
			switch (type) {
			case 0://mean
				double dSum=0.0;
				for (DoubleWritable e : values) {
					dSum+=e.get();
					nums++;
				}
				preference=dSum/nums;
				break;
				
			case 1://median
				
				break;

			default:
				break;
			}
			return preference;
			
		}

		@Override
		protected void reduce(NullWritable key,
				Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {
			
			
			// write it out
			context.write(NullWritable.get(), new DoubleWritable(getPreference(values)));
		}
	}
}
