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
import org.apache.mahout.clustering.spectral.common.MatrixDiagonalizeJob;
import org.apache.mahout.clustering.spectral.common.VectorCache;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 
* @author Tang
* @since 2012-3
* 获取矩阵的对角阵
* */
public final class GetMatrixDiag {
	private static final Logger log = LoggerFactory
			.getLogger(GetMatrixDiag.class);

	public static Vector runJob(Path affInput, int dimensions)
			throws IOException, ClassNotFoundException, InterruptedException {

		// set up all the job tasks
		Configuration conf = new Configuration();
		Path diagOutput = new Path(affInput.getParent(), "diagonal"+ (System.nanoTime() & 0xFF));
		HadoopUtil.delete(conf, diagOutput);
		conf.setInt(APCMatrixInputJob.MATRIX_DIMENSIONS, dimensions);
		Job job = new Job(conf, "MatrixDiagonalizeJob");

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(IntDoublePairWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(VectorWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setMapperClass(MatrixDiagonalizeMapper.class);
		job.setReducerClass(MatrixDiagonalizeReducer.class);

		FileInputFormat.addInputPath(job, affInput);
		FileOutputFormat.setOutputPath(job, diagOutput);

		job.setJarByClass(MatrixDiagonalizeJob.class);

		job.waitForCompletion(true);
		Vector vt = VectorCache
				.load(conf, new Path(diagOutput, "part-r-00000"));


		// read the results back from the path
		return vt;
	}

	public static class MatrixDiagonalizeMapper
			extends
			Mapper<IntWritable, VectorWritable, NullWritable, IntDoublePairWritable> {

		@Override
		protected void map(IntWritable key, VectorWritable row, Context context)
				throws IOException, InterruptedException {
			// store the sum
			IntDoublePairWritable store = new IntDoublePairWritable(key.get(),
					row.get().getQuick(key.get()));
			context.write(NullWritable.get(), store);
		}
	}

	public static class MatrixDiagonalizeReducer
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
				retval.setQuick(e.getKey(), e.getValue());
			}
			// write it out
			context.write(key, new VectorWritable(retval));
		}
	}
}
