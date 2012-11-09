/**
 * 
 */
package org.swjtu.helloworldcn;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Writable;
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
import org.apache.mahout.math.function.Functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Tang 2012 Get the clustering result
 */
public final class APCGetClusteringResultJob {
	private static final Logger log = LoggerFactory
			.getLogger(APCGetClusteringResultJob.class);
	public static final String output_ExemplasPath = "org.swjtu.helloworldcn.output_ExemplasPath";

	public static Vector runJob(Path affInput, Path outputExemplasPath,
			int dimensions) throws IOException, ClassNotFoundException,
			InterruptedException {

		// set up all the job tasks
		Configuration conf = new Configuration();
		Path outputPath = new Path(affInput.getParent(),
				"APCGetClusteringResult1" + (System.nanoTime() & 0xFF));
		HadoopUtil.delete(conf, outputPath);
		conf.setInt(APCMatrixInputJob.MATRIX_DIMENSIONS, dimensions);
		conf.set(APCGetClusteringResultJob.output_ExemplasPath,
				outputExemplasPath.toUri().toString());
		Job job1 = new Job(conf, "APCGetClusteringResult");

		job1.setInputFormatClass(SequenceFileInputFormat.class);
		job1.setMapOutputKeyClass(IntWritable.class);
		job1.setMapOutputValueClass(APCResultRowWritable.class);
		job1.setOutputKeyClass(NullWritable.class);
		job1.setOutputValueClass(IntWritable.class);
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);
		job1.setMapperClass(APCGetClusteringResultMapper1.class);
		job1.setReducerClass(APCGetClusteringResultReducer1.class);

		FileInputFormat.addInputPath(job1, affInput);
		FileOutputFormat.setOutputPath(job1, outputPath);

		job1.setJarByClass(APCGetClusteringResultJob.class);

		job1.waitForCompletion(true);
		ArrayList<Integer> ex=new ArrayList<Integer>();
		
		FileSystem fs = FileSystem.get(conf);
		SequenceFile.Reader reader = new Reader(fs, new Path(outputPath, "part-r-00000"), conf);
		IntWritable value = new IntWritable();

		while (reader.next(NullWritable.get(), value)) {
			ex.add(value.get());
		}
		
		
		conf.setInt(APCMatrixInputJob.MATRIX_DIMENSIONS, dimensions);
		conf.set(APCGetClusteringResultJob.output_ExemplasPath,
				(new Path(outputPath, "part-r-00000")).toUri().toString());
		Job job2 = new Job(conf, "APCGetClusteringResult");
		outputPath = new Path(affInput.getParent(),
				"APCGetClusteringResult2" + (System.nanoTime() & 0xFF));
		HadoopUtil.delete(conf, outputPath);
		job2.setInputFormatClass(SequenceFileInputFormat.class);
		job2.setMapOutputKeyClass(NullWritable.class);
		job2.setMapOutputValueClass(IntDoublePairWritable.class);
		job2.setOutputKeyClass(NullWritable.class);
		job2.setOutputValueClass(VectorWritable.class);
		job2.setOutputFormatClass(SequenceFileOutputFormat.class);
		job2.setMapperClass(APCGetClusteringResultMapper.class);
		job2.setReducerClass(APCGetClusteringResultReducer.class);

		FileInputFormat.addInputPath(job2, affInput);
		FileOutputFormat.setOutputPath(job2, outputPath);

		job2.setJarByClass(APCGetClusteringResultJob.class);

		job2.waitForCompletion(true);
		
		
		
		Vector vt = VectorCache
				.load(conf, new Path(outputPath, "part-r-00000"));
		/*for (Iterator iterator = ex.iterator(); iterator.hasNext();) {
			Integer val = (Integer) iterator.next();
			vt.setQuick(val, val);			
		}*/
		System.out.println("clustering result idx:" + vt);

		// read the results back from the path
		return vt;
	}

	public static class APCGetClusteringResultMapper1
			extends
			Mapper<IntWritable, APCRowVectorWritable, IntWritable, APCResultRowWritable> {

		private int colnums;
		ArrayList<Integer> exemplars = new ArrayList<Integer>();

		@Override
		protected void map(IntWritable key, APCRowVectorWritable row,
				Context context) throws IOException, InterruptedException {
			int maxIndex = 0;
			double maxValue = -Double.MAX_VALUE;
			for (int i = 0; i < exemplars.size(); i++) {
				if (exemplars.get(i)==key.get()) {
					maxIndex=i;
					break;
				}
				if (row.getVectorS().get(exemplars.get(i)) > maxValue) {
					maxIndex = i;
					maxValue = row.getVectorS().get(exemplars.get(i));
				}
			}
			APCResultRowWritable storeApcResultRowWritable = new APCResultRowWritable(
					row.getVectorS(), key.get());
			context.write(new IntWritable(exemplars.get(maxIndex)),
					storeApcResultRowWritable);

			/*
			 * // store clustering result IntDoublePairWritable store = new
			 * IntDoublePairWritable(key.get(), exemplars.get(maxIndex));
			 * context.write(NullWritable.get(), store);
			 */
		}

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {

			super.setup(context);

			Configuration config = context.getConfiguration();

			colnums = Integer.parseInt(config
					.get(APCMatrixInputJob.MATRIX_DIMENSIONS));
			Path exPath = new Path(URI.create(config
					.get(APCGetClusteringResultJob.output_ExemplasPath)));

			FileSystem fs = FileSystem.get(config);
			SequenceFile.Reader reader = new Reader(fs, exPath, config);
			IntWritable value = new IntWritable();

			while (reader.next(NullWritable.get(), value)) {
				exemplars.add(value.get());
			}
		}
	}

	public static class APCGetClusteringResultMapper
			extends
			Mapper<IntWritable, APCRowVectorWritable, NullWritable, IntDoublePairWritable> {

		private int colnums;
		ArrayList<Integer> exemplars = new ArrayList<Integer>();

		@Override
		protected void map(IntWritable key, APCRowVectorWritable row,
				Context context) throws IOException, InterruptedException {
			int maxIndex = 0;
			double maxValue = -Double.MAX_VALUE;
			for (int i = 0; i < exemplars.size(); i++) {
				if (exemplars.get(i)==key.get()) {
					maxIndex=i;
					break;
				}
				if (row.getVectorS().get(exemplars.get(i)) > maxValue) {
					maxIndex = i;
					maxValue = row.getVectorS().get(exemplars.get(i));
				}
			}

			// store clustering result
			IntDoublePairWritable store = new IntDoublePairWritable(key.get(),
					exemplars.get(maxIndex));
			context.write(NullWritable.get(), store);
		}

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {

			super.setup(context);

			Configuration config = context.getConfiguration();

			colnums = Integer.parseInt(config
					.get(APCMatrixInputJob.MATRIX_DIMENSIONS));
			Path exPath = new Path(URI.create(config
					.get(APCGetClusteringResultJob.output_ExemplasPath)));

			FileSystem fs = FileSystem.get(config);
			SequenceFile.Reader reader = new Reader(fs, exPath, config);
			IntWritable value = new IntWritable();

			while (reader.next(NullWritable.get(), value)) {
				exemplars.add(value.get());
			}
		}
	}

	public static class APCGetClusteringResultReducer1
			extends
			Reducer<IntWritable, APCResultRowWritable, NullWritable, IntWritable> {

		@Override
		protected void reduce(IntWritable key,
				Iterable<APCResultRowWritable> values, Context context)
				throws IOException, InterruptedException {
			Vector retval = new DenseVector(context.getConfiguration().getInt(
					APCMatrixInputJob.MATRIX_DIMENSIONS, Integer.MAX_VALUE));
			
			ArrayList<Integer> elementIndex=new ArrayList<Integer>();
			
			for (APCResultRowWritable e : values) {
				if (elementIndex.isEmpty()) {
					retval.assign(e.getVectorS());
				}else {
					retval.assign(e.getVectorS(), Functions.PLUS);
				}
				elementIndex.add(e.getKey());
			}
			int maxIndex=0;
			double maxValue = -Double.MAX_VALUE;
			for (Integer val : elementIndex) {
				if (retval.getQuick(val)>maxValue) {
					maxIndex=val;
					maxValue=retval.getQuick(val);
				}
			}
			// write it out
			context.write(NullWritable.get(), new IntWritable(maxIndex));
		}

	}

	public static class APCGetClusteringResultReducer
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

final class APCResultRowWritable implements Writable {

	private final VectorWritable S = new VectorWritable();
	private int key;

	/**
	 * @return the key
	 */
	public int getKey() {
		return key;
	}

	/**
	 * @param key
	 *            the key to set
	 */
	public void setKey(int key) {
		this.key = key;
	}

	public APCResultRowWritable() {
	}

	public APCResultRowWritable(Vector s, int key) {

		this.S.set(s);
		this.key = key;
	}

	public Vector getVectorS() {
		return S.get();
	}

	public void setVectorS(Vector vector) {
		S.set(vector);
	}

	public void readFields(DataInput in) {
		try {
			this.key = in.readInt();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			S.readFields(in);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(key);
		S.write(out);

	}

	public static APCResultRowWritable read(DataInput in) throws IOException {
		APCResultRowWritable writable = new APCResultRowWritable();
		writable.readFields(in);
		return writable;
	}

}
