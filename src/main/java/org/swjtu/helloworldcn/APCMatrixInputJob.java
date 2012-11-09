/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.swjtu.helloworldcn;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.HadoopUtil;

/**
 * @author Tang
 * @since 2012-03
 * read input data, and Construct sparse similarity matrix for clustering algorithm 
 * 对数据进行预处理
 */
public final class APCMatrixInputJob {
	public final static String MATRIX_DIMENSIONS = "org.swjtu.helloworldcn.MATRIX_DIMENSIONS";

	public final static String TEMPORARY_SAVE_PREFERENCE = "org.swjtu.helloworldcn.TEMPORARY_SAVE_PREFERENCE";
	public final static String TEMPORARY_SAVE_NONOISE = "org.swjtu.helloworldcn.TEMPORARY_SAVE_NONOISE";

	private APCMatrixInputJob() {
	}

	/**
	 * Initializes and executes the job of reading the documents containing the
	 * data of the similarly matrix in (i, j, value) format.
	 */
	public static void runJob(Path input, Path output, int rows, int cols,double preference,int nonoise)
			throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		HadoopUtil.delete(conf, output);

		conf.setInt(MATRIX_DIMENSIONS, rows);
/*		conf.setInt("io.sort.mb", 400);
		conf.setInt("io.sort.factor", 100);

		//conf.setInt("mapred.reduce.parallel.copies", 20);
		conf.setInt("io.sort.mb", 100);
		//conf.setFloat("mapred.job.reduce.input.buffer.percent", 0.5f);
		conf.setBoolean("mapred.output.compress",true);
		conf.setClass("mapred.output.compression.codec",GzipCodec.class, CompressionCodec.class);*/

		conf.set(TEMPORARY_SAVE_PREFERENCE, new Double(preference).toString());
		conf.set(TEMPORARY_SAVE_NONOISE, new Integer(nonoise).toString());
		Job job = new Job(conf, "APCMatrixInputJob: " + input + " -> M/R -> "
				+ output);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(APCMatrixEntryWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(APCRowVectorWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setMapperClass(APCMatrixInputMapper.class);
		job.setReducerClass(APCMatrixInputReducer.class);

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);

		job.setJarByClass(APCMatrixInputJob.class);

		job.waitForCompletion(true);
	}

}
