/**
 * 
 */
package org.swjtu.helloworldcn;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.clustering.spectral.common.IntDoublePairWritable;
import org.apache.mahout.clustering.spectral.common.VectorCache;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Tang 
 * @since 2012-03
 * Get result of matrix A's diag plus matrix R's diag
 * 得到A的对角矩阵加上R的对角阵
 */
public final class APCGetDiagAplusRJob {
	private static final Logger log = LoggerFactory
			.getLogger(APCGetDiagAplusRJob.class);

	public static Vector runJob(Path affInput, int dimensions)
			throws IOException, ClassNotFoundException, InterruptedException {

		// set up all the job tasks
		Configuration conf = new Configuration();
		Path diagOutput = new Path(affInput.getParent(), "diagonal"+ (System.nanoTime() & 0xFF));
		HadoopUtil.delete(conf, diagOutput);
		conf.setInt(APCMatrixInputJob.MATRIX_DIMENSIONS, dimensions);
		Job job = new Job(conf, "APCGetDiagAplusR");

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(IntDoublePairWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(VectorWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setMapperClass(APCGetDiagAplusRMapper.class);
		job.setReducerClass(APCGetDiagAplusRReducer.class);

		FileInputFormat.addInputPath(job, affInput);
		FileOutputFormat.setOutputPath(job, diagOutput);

		job.setJarByClass(APCGetDiagAplusRJob.class);

		job.waitForCompletion(true);
		Vector vt = VectorCache
				.load(conf, new Path(diagOutput, "part-r-00000"));

		// read the results back from the path
		return vt;
	}

	public static class APCGetDiagAplusRMapper
			extends
			Mapper<IntWritable, APCRowVectorWritable, NullWritable, IntDoublePairWritable> {

		@Override
		protected void map(IntWritable key, APCRowVectorWritable row, Context context)
				throws IOException, InterruptedException {
			
			// store the sum
			IntDoublePairWritable store = new IntDoublePairWritable(key.get(),
					row.getVectorA().getQuick(key.get())+row.getVectorR().getQuick(key.get()));
			//System.out.println(store);
			context.write(NullWritable.get(), store);
		}
	}

	public static class APCGetDiagAplusRReducer
			extends
			Reducer<NullWritable, IntDoublePairWritable, NullWritable, VectorWritable> {

		@Override
		protected void reduce(NullWritable key,
				Iterable<IntDoublePairWritable> values, Context context)
				throws IOException, InterruptedException {
			// create the return vector
			Vector retval = new DenseVector(context.getConfiguration().getInt(
					APCMatrixInputJob.MATRIX_DIMENSIONS, Integer.MAX_VALUE));
			// put everything in its correct spot
			for (IntDoublePairWritable e : values) {
				//System.out.println(e.getValue());
				retval.setQuick(e.getKey(), e.getValue());
			}
			// write it out
			context.write(key, new VectorWritable(retval));
		}
	}
}
