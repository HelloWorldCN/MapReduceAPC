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
import java.net.URI;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.math.MatrixSlice;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.hadoop.DistributedRowMatrix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 
* @author Tang
* @since 2012-3
* */
@Deprecated
public final class ComputeResponsibilitiesJob {
	public final static String A_PATH = "A_PATH";
	public final static String ST_PATH = "ST_PATH";
	public final static String TMP_ST_PATH = "TMP_ST_PATH";
	public final static String TMP_A_PATH = "TMP_A_PATH";
	public final static String COL_NUMS = "COL_NUMS";
	private static final Logger log = LoggerFactory.getLogger(ComputeResponsibilitiesJob.class);

	private ComputeResponsibilitiesJob() {
	}



	public static DistributedRowMatrix runJob(DistributedRowMatrix A,
			DistributedRowMatrix R, DistributedRowMatrix st,Path outputPath,Path tmpPath)
			throws IOException, ClassNotFoundException, InterruptedException {

		// set up the serialization of the diagonal vector
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path RPath=fs.makeQualified(R.getRowPath());
		
		
		conf.set(ST_PATH,st.getRowPath().toUri().toString());
		conf.set(A_PATH, A.getRowPath().toUri().toString());
		conf.set(TMP_ST_PATH, st.getOutputTempPath().toUri().toString());
		conf.set(TMP_A_PATH, A.getOutputTempPath().toUri().toString());
		conf.set(COL_NUMS, A.numRows()+"");


		outputPath = fs.makeQualified(outputPath);

		/*
		 * VectorCache.save(new IntWritable(EigencutsKeys.DIAGONAL_CACHE_INDEX),
		 * diag, vectorOutputPath, conf);
		 */

		// set up the job itself
		Job job = new Job(conf, "ComputeResponsibilities");
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(VectorWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setMapperClass(ComputeResponsibilitiesMapper.class);
		job.setNumReduceTasks(0);

		FileInputFormat.addInputPath(job, RPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.setJarByClass(ComputeResponsibilitiesJob.class);

		job.waitForCompletion(true);

		// build the resulting DRM from the results
		return new DistributedRowMatrix(outputPath, tmpPath, R.numRows(),
				R.numCols());
	}

	public static class ComputeResponsibilitiesMapper extends
			Mapper<IntWritable, VectorWritable, IntWritable, VectorWritable> {

		private DistributedRowMatrix A;
		private DistributedRowMatrix st;
		private int colnums;
		private Iterator<MatrixSlice> iteratorst;
		private Iterator<MatrixSlice>iteratorA;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// read in the diagonal vector from the distributed cache
			super.setup(context);
			
			Configuration config = context.getConfiguration();
			log.info("TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT  "+config.get(ComputeResponsibilitiesJob.A_PATH));
			Path stPath=new Path(URI.create(config.get(ComputeResponsibilitiesJob.ST_PATH)));
			Path APath=new Path(URI.create(config.get(ComputeResponsibilitiesJob.A_PATH)));
			Path sttmpPath=new Path(URI.create(config.get(ComputeResponsibilitiesJob.TMP_ST_PATH)));
			Path AtmpPath=new Path(URI.create(config.get(ComputeResponsibilitiesJob.TMP_A_PATH)));
			colnums=Integer.parseInt(config.get(ComputeResponsibilitiesJob.COL_NUMS));
			Configuration depConf = new Configuration(config);
			
			st = new DistributedRowMatrix(stPath,
                    sttmpPath,
                    colnums,
                    colnums);
			A = new DistributedRowMatrix(APath,
                    AtmpPath,
                    colnums,
                    colnums);
			A.setConf(config);
			st.setConf(config);
			iteratorst=st.iterateAll();
			iteratorA=A.iterateAll();
			
		}

		@Override
		protected void map(IntWritable key, VectorWritable row, Context ctx)
				throws IOException, InterruptedException {
			log.info("ddddddddddddddddddddddddddddddddd");
			Vector rVector=row.get();
			log.info(rVector.toString());
			int rownum=key.get();
			Vector strowVector=null;
			Vector aRowVector=null;
			
			while (strowVector==null) {
				while (iteratorst.hasNext()) {
					MatrixSlice matrixSlice = iteratorst.next();
					if (matrixSlice.index()==rownum) {
						strowVector=matrixSlice.vector();
						log.info("Get s[i,]  "+matrixSlice.index()+strowVector);
						break;
					}								
				}
				//not find, refind from first
				if (strowVector==null) {
					iteratorst=st.iterateAll();
				}				
			}
		    
		    while (aRowVector==null) {
		    	while (iteratorA.hasNext()) {
					MatrixSlice matrixSlice = iteratorA.next();
					if (matrixSlice.index()==rownum) {
						aRowVector=matrixSlice.vector();
						log.info("Get A[i,] "+matrixSlice.index()+aRowVector);
						break;
					}								
				}
		    	if (aRowVector==null) {
					iteratorA=A.iterateAll();
				}
				
			}
		    
		    aRowVector=aRowVector.plus(strowVector);
		    int I=aRowVector.maxValueIndex();
		    double Y=aRowVector.get(I);
		    aRowVector.set(I, -Double.MAX_VALUE);
		   
            
            int I2 = aRowVector.maxValueIndex();
            double Y2 = aRowVector.get(I2);
            
            rVector.assign(strowVector.plus(-Y));
            rVector.set(I, strowVector.get(I)-Y2);
            rVector=rVector.times(1-0.5).plus(row.get().times(0.5));
            row.set(rVector);
            
            
			ctx.write(key, row);
		}

	}
}
