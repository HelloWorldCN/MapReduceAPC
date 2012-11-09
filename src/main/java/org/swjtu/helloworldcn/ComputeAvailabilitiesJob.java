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
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.hadoop.DistributedRowMatrix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/** 
* @author Tang
* @since 2012-3
* */
@Deprecated
public final class ComputeAvailabilitiesJob {
	public final static String R_PATH = "R_PATH";
	public final static String ST_PATH = "ST_PATH";
	public final static String TMP_ST_PATH = "TMP_ST_PATH";
	public final static String TMP_R_PATH = "TMP_R_PATH";
	public final static String COL_NUMS = "COL_NUMS";
	private static final Logger log = LoggerFactory.getLogger(ComputeAvailabilitiesJob.class);

	private ComputeAvailabilitiesJob() {
	}



	public static DistributedRowMatrix runJob(DistributedRowMatrix A,
			DistributedRowMatrix R, DistributedRowMatrix st,Path outputPath,Path tmpPath)
			throws IOException, ClassNotFoundException, InterruptedException {

		// set up the serialization of the diagonal vector
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path APath=fs.makeQualified(A.getRowPath());
		
		
		conf.set(ST_PATH,st.getRowPath().toUri().toString());
		conf.set(R_PATH, A.getRowPath().toUri().toString());
		conf.set(TMP_ST_PATH, st.getOutputTempPath().toUri().toString());
		conf.set(TMP_R_PATH, A.getOutputTempPath().toUri().toString());
		conf.set(COL_NUMS, A.numRows()+"");


		outputPath = fs.makeQualified(outputPath);

		/*
		 * VectorCache.save(new IntWritable(EigencutsKeys.DIAGONAL_CACHE_INDEX),
		 * diag, vectorOutputPath, conf);
		 */

		// set up the job itself
		Job job = new Job(conf, "ComputeAvailabilities");
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(VectorWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setMapperClass(ComputeAvailabilitiesMapper.class);
		job.setNumReduceTasks(0);

		FileInputFormat.addInputPath(job, APath);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.setJarByClass(ComputeAvailabilitiesJob.class);

		job.waitForCompletion(true);

		// build the resulting DRM from the results
		return new DistributedRowMatrix(outputPath, tmpPath, A.numRows(),
				A.numCols());
	}

	public static class ComputeAvailabilitiesMapper extends
			Mapper<IntWritable, VectorWritable, IntWritable, VectorWritable> {

		private DistributedRowMatrix R;
		private DistributedRowMatrix st;
		private int colnums;
		private Iterator<MatrixSlice> iteratorst;
		private Iterator<MatrixSlice>iteratorR;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// read in the diagonal vector from the distributed cache
			super.setup(context);
			
			Configuration config = context.getConfiguration();
			Path stPath=new Path(URI.create(config.get(ComputeAvailabilitiesJob.ST_PATH)));
			Path RPath=new Path(URI.create(config.get(ComputeAvailabilitiesJob.R_PATH)));
			Path sttmpPath=new Path(URI.create(config.get(ComputeAvailabilitiesJob.TMP_ST_PATH)));
			Path RtmpPath=new Path(URI.create(config.get(ComputeAvailabilitiesJob.TMP_R_PATH)));
			colnums=Integer.parseInt(config.get(ComputeAvailabilitiesJob.COL_NUMS));
			Configuration depConf = new Configuration(config);
			
			st = new DistributedRowMatrix(stPath,
                    sttmpPath,
                    colnums,
                    colnums);
			R = new DistributedRowMatrix(RPath,
                    RtmpPath,
                    colnums,
                    colnums);
			R.setConf(config);
			st.setConf(config);
			iteratorst=st.iterateAll();
			iteratorR=R.iterateAll();
			
		}

		@Override
		protected void map(IntWritable key, VectorWritable row, Context ctx)
				throws IOException, InterruptedException {
			System.out.println("XXXXXXXXXXXXXXXXXXXXXXX A ji suan hang shuju");
			Vector aVector=row.get();
			int rownum=key.get();
			Vector strowVector=null;
			Vector rRowVector=null;
			
			while (strowVector==null) {
				while (iteratorst.hasNext()) {
					MatrixSlice matrixSlice = iteratorst.next();
					if (matrixSlice.index()==rownum) {
						strowVector=matrixSlice.vector();
						break;
					}								
				}
				//not find, refind from first
				if (strowVector==null) {
					iteratorst=st.iterateAll();
				}				
			}
		    
		    while (rRowVector==null) {
		    	while (iteratorR.hasNext()) {
					MatrixSlice matrixSlice = iteratorR.next();
					if (matrixSlice.index()==rownum) {
						rRowVector=matrixSlice.vector();
						break;
					}								
				}
		    	if (rRowVector==null) {
					iteratorR=R.iterateAll();
				}
				
			}
		    Iterator<Element> iter = rRowVector.iterateNonZero();
		    while (iter.hasNext()) {
		      Element element = iter.next();
		      if (element.index()==rownum) {
					continue;
		      }
		      if (element.get()<0.0) {
		    	  element.set(0.0);
		      }
		    }
		   
            double rSum=rRowVector.zSum();
            for (int i = 0; i < aVector.size(); i++) {
				aVector.setQuick(i, rSum-rRowVector.getQuick(i));
			}
            
            double dA = aVector.get(rownum);
            Iterator<Element> itera = aVector.iterateNonZero();
		    while (itera.hasNext()) {
		      Element element = itera.next();
		      if (element.index()==rownum) {
					continue;
		      }
		      if (element.get()>0) {
		    	  element.set(0.0);
		      }
		    }
            aVector.set(rownum, dA);
            aVector=aVector.times(1-0.5).plus(row.get().times(0.5));
            row.set(aVector);
            
            System.out.println("XXXXXXXXXXXXXXXXXXXXXXX xie ru shu ju"+aVector);
			ctx.write(key, row);
		}

	}
}
