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
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * This class parallel update the responsibility and availability of nodes 
 * @author Tang
 * @since 2012-3
 * 分布式并行更新数据的A和R
 */
public final class APCParallelUpdateRAJob {

	public final static String COL_NUMS = "org.swjtu.helloworldcn.APCParallelUpdateRAJob.COL_NUMS";
	public final static String LAMDA = "org.swjtu.helloworldcn.APCParallelUpdateRAJob.LAMDA";
	private static final Logger log = LoggerFactory
			.getLogger(APCParallelUpdateRAJob.class);

	private APCParallelUpdateRAJob() {
	}

	public static Path runJob(Path inputPath, Path outputPath, 
			int colnums,double lamda) throws IOException, ClassNotFoundException,
			InterruptedException {

		// set up the parameters of runtime for jobs
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		inputPath = fs.makeQualified(inputPath);

		conf.set(COL_NUMS, colnums + "");
		conf.set(LAMDA, String.valueOf(lamda));
/*		conf.setInt("io.sort.mb", 400);
		conf.setInt("io.sort.factor", 100);

		//conf.setInt("mapred.reduce.parallel.copies", 20);
		//conf.setFloat("mapred.job.reduce.input.buffer.percent", 0.5f);
		conf.setBoolean("mapred.output.compress",true);
		conf.setClass("mapred.output.compression.codec",GzipCodec.class, CompressionCodec.class);*/
	
		

		outputPath = fs.makeQualified(outputPath);

		// set up the APCParallelUpdateRAJob job 
		Job jobUpdate = new Job(conf, "APCParallelUpdateRAJob");

		jobUpdate.setInputFormatClass(SequenceFileInputFormat.class);
		jobUpdate.setOutputKeyClass(IntWritable.class);
		jobUpdate.setOutputValueClass(APCMatrixEntryWritable.class);
		jobUpdate.setOutputFormatClass(SequenceFileOutputFormat.class);
		jobUpdate.setMapperClass(APCParallelUpdateRAMapper.class);
		jobUpdate.setReducerClass(APCParallelUpdateRAReducer.class);
		Path outputUpdatePath=new Path(outputPath,"tmp");
		FileInputFormat.addInputPath(jobUpdate, inputPath);
		FileOutputFormat.setOutputPath(jobUpdate,outputUpdatePath );

		jobUpdate.setJarByClass(APCParallelUpdateRAJob.class);

		jobUpdate.waitForCompletion(true);

		// set up the translate matrix job 
		Job jobTrans = new Job(conf, "APCParallelUpdateRATransJob");

		jobTrans.setInputFormatClass(SequenceFileInputFormat.class);
		//jobTrans.setOutputKeyClass(IntWritable.class);
		//jobTrans.setOutputValueClass(APCRowVectorWritable.class);
		jobTrans.setOutputFormatClass(SequenceFileOutputFormat.class);
		jobTrans.setMapperClass(APCParallelUpdateRATransposeMapper.class);
		jobTrans.setReducerClass(APCParallelUpdateRATransposeReducer.class);
		jobTrans.setMapOutputKeyClass(IntWritable.class);
		jobTrans.setMapOutputValueClass(APCMatrixEntryWritable.class);
		jobTrans.setOutputKeyClass(IntWritable.class);
		jobTrans.setOutputValueClass(APCRowVectorWritable.class);

		FileInputFormat.addInputPath(jobTrans, outputUpdatePath);
		outputPath=new Path(outputPath,"result"); 
		FileOutputFormat.setOutputPath(jobTrans, outputPath);

		jobTrans.setJarByClass(APCParallelUpdateRAJob.class);

		jobTrans.waitForCompletion(true);

		return outputPath;

	}

	public static class APCParallelUpdateRAMapper
			extends
			Mapper<IntWritable, APCRowVectorWritable, IntWritable, APCMatrixEntryWritable> {



		private double lamda;

		private int colnums;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			
			super.setup(context);

			Configuration config = context.getConfiguration();
			// get config's parameters
			colnums = Integer.parseInt(config
					.get(APCParallelUpdateRAJob.COL_NUMS));
			lamda=Double.parseDouble(config.get(APCParallelUpdateRAJob.LAMDA));

		}

		@Override
		protected void map(IntWritable key, APCRowVectorWritable row,
				Context ctx) throws IOException, InterruptedException {
			// compute responsibility
			//Vector aRowVector = new RandomAccessSparseVector(row.getVectorA());
			//Vector rVector = new RandomAccessSparseVector(row.getVectorR());
			//Vector strowVector = new RandomAccessSparseVector(row.getVectorS());
			int rownum = key.get();

/*	
 * old 1
 * 		aRowVector = aRowVector.plus(row.getVectorS());
			System.out.println(aRowVector.toString());
			int I = aRowVector.maxValueIndex();
			double Y = aRowVector.get(I);
			aRowVector.set(I, Double.NEGATIVE_INFINITY);

			int I2 = aRowVector.maxValueIndex();
			double Y2 = aRowVector.get(I2);

			rVector.assign(row.getVectorS().plus(-Y));
			rVector.set(I, row.getVectorS().get(I) - Y2);
			rVector = rVector.times(1 - lamda).plus(row.getVectorR().times(lamda));*/
							
			
			//optimization
			Iterator<Element> iter = row.getVectorS().iterateNonZero();
			int indexI = -1;
			double maxY = Double.NEGATIVE_INFINITY;
			double maxY2 = Double.NEGATIVE_INFINITY;
			while (iter.hasNext()) {
				Element element = iter.next();
				double tmpValue=element.get()+row.getVectorA().get(element.index());
				//System.out.print(tmpValue+",");
				if (tmpValue>maxY) {
					maxY2=maxY;
					
					maxY=tmpValue;
					indexI=element.index();
				}else if (tmpValue>maxY2) {
					maxY2=tmpValue;
				}
			}

			iter = row.getVectorS().iterateNonZero();
			while (iter.hasNext()) {
				Element element = iter.next();
				int j=element.index();
				if (j==indexI) {
					row.getVectorR().set(j, 
							(element.get()-maxY2)*(1-lamda)+row.getVectorR().get(j)*lamda);
				}else {
					row.getVectorR().set(j, 
							(element.get()-maxY)*(1-lamda)+row.getVectorR().get(j)*lamda);
				}
				APCMatrixEntryWritable entryWritable = new APCMatrixEntryWritable();
				entryWritable.setCol(rownum);
				entryWritable.setRow(j);
				entryWritable.setValA(row.getVectorA().getQuick(j));
				entryWritable.setValS(row.getVectorS().getQuick(j));
				entryWritable.setValR(row.getVectorR().getQuick(j));
				ctx.write(new IntWritable(j), entryWritable);
				
			}
			//System.out.println("responsibility1"+rVector.toString());
			//System.out.println("responsibility2"+row.getVectorR().toString());
			
			
			
			//System.out.println("responsibility"+rVector.toString());
			/*
			 * old 1
			 * iter = row.getVectorS().iterateNonZero();
			while (iter.hasNext()) {
				Element element = iter.next();
				int j=element.index();
				APCMatrixEntryWritable entryWritable = new APCMatrixEntryWritable();
				entryWritable.setCol(rownum);
				entryWritable.setRow(j);
				entryWritable.setValA(row.getVectorA().getQuick(j));
				entryWritable.setValS(row.getVectorS().getQuick(j));
				entryWritable.setValR(rVector.get(j));
				ctx.write(new IntWritable(j), entryWritable);
			}*/
			
			
			//System.out.println("compute responsibility"+rVector);
			/*for (int j = 0; j < colnums; j++) {
				APCMatrixEntryWritable entryWritable = new APCMatrixEntryWritable();
				entryWritable.setCol(rownum);
				entryWritable.setRow(j);
				entryWritable.setValA(row.getVectorA().getQuick(j));
				entryWritable.setValS(row.getVectorS().getQuick(j));
				entryWritable.setValR(rVector.get(j));
				ctx.write(new IntWritable(j), entryWritable);
			}*/
		}

	}

	public static class APCParallelUpdateRAReducer
			extends
			Reducer<IntWritable, APCMatrixEntryWritable, IntWritable, APCMatrixEntryWritable> {
		private int colnums;
		private double lamda;

		@Override
		protected void reduce(IntWritable key,
				Iterable<APCMatrixEntryWritable> values, Context context)
				throws IOException, InterruptedException {
			// compute availability
			Vector rRowVector = new RandomAccessSparseVector(colnums, 100);
			Vector aVector = new RandomAccessSparseVector(colnums, 100);
			Vector stRowVector = new RandomAccessSparseVector(colnums, 100);
			Vector rOldVector= new RandomAccessSparseVector(colnums, 100);
			int rownum = key.get();
			double rSum=0.0;
			for (APCMatrixEntryWritable element : values) {
				rOldVector.setQuick(element.getCol(), element.getValR());
				
				if (element.getCol()==rownum) {
					rSum+=element.getValR();
					rRowVector.setQuick(element.getCol(), element.getValR());
				}else if (element.getValR()>0.0) {
					rSum+=element.getValR();
					rRowVector.setQuick(element.getCol(), element.getValR());
				}else if (element.getValR()<0.0) {
					rRowVector.setQuick(element.getCol(), 0.0);
				}
				aVector.setQuick(element.getCol(), element.getValA());
				stRowVector.setQuick(element.getCol(), element.getValS());
			}
			//Vector aOldVector = new RandomAccessSparseVector(aVector);
			//Vector rOldVector= new RandomAccessSparseVector(rRowVector);
			
			/*
			 * old 1
			 * Iterator<Element> iter = rRowVector.iterateNonZero();
			while (iter.hasNext()) {
				Element element = iter.next();
				if (element.index()==rownum) {
					continue;
		      }
				if (element.get() < 0.0) {
					rRowVector.setQuick(element.index(), 0.0);
				} 
			}*/
			
			
			/*for (int i = 0; i < colnums; i++) {
				if (i==rownum) {
					continue;
		      }
				if (rRowVector.getQuick(i) < 0.0) {
					rRowVector.setQuick(i, 0.0);
				} else {

				}
			}*/
			
			//double rSum = rRowVector.zSum();
			//System.out.println(rSum+","+zSum);
			
			//optimization
			Iterator<Element> itera = stRowVector.iterateNonZero();
			while (itera.hasNext()) {
				Element element = itera.next();
				int i=element.index();
				double tmp=rSum - rRowVector.getQuick(i);
				if (rownum!=i) {
					if (tmp>0) {
						tmp=0.0;
					}	
				}
				tmp=tmp*(1-lamda)+aVector.get(i)*lamda;
				APCMatrixEntryWritable entryWritable = new APCMatrixEntryWritable();
				entryWritable.setCol(rownum);
				entryWritable.setRow(i);
				entryWritable.setValA(tmp);
				entryWritable.setValS(stRowVector.getQuick(i));
				entryWritable.setValR(rOldVector.get(i));
				context.write(new IntWritable(i), entryWritable);	
			}
			
			
			
			
			
			/*
			 * old 1
			 * for (int i = 0; i < aVector.size(); i++) {
				aVector.setQuick(i, rSum - rRowVector.getQuick(i));
			}

			double dA = aVector.get(rownum);
			itera = aVector.iterateNonZero();

			while (itera.hasNext()) {
				Element element = itera.next();
				if (element.index() == rownum) {
					continue;
				}
				if (element.get() > 0) {
					aVector.setQuick(element.index(), 0.0);
				}
			}
			aVector.set(rownum, dA);
			aVector = aVector.times(1 - lamda).plus(aOldVector.times(lamda));
			System.out.println(aVector);

			
			itera = stRowVector.iterateNonZero();
			while (itera.hasNext()) {
				Element element = itera.next();
				int j=element.index();
				APCMatrixEntryWritable entryWritable = new APCMatrixEntryWritable();
				entryWritable.setCol(rownum);
				entryWritable.setRow(j);
				entryWritable.setValA(aVector.getQuick(j));
				entryWritable.setValS(stRowVector.getQuick(j));
				entryWritable.setValR(rOldVector.get(j));
				context.write(new IntWritable(j), entryWritable);
			}*/
			
			
			/*for (int j = 0; j < colnums; j++) {
				APCMatrixEntryWritable entryWritable = new APCMatrixEntryWritable();
				entryWritable.setCol(rownum);
				entryWritable.setRow(j);
				entryWritable.setValA(aVector.getQuick(j));
				entryWritable.setValS(stRowVector.getQuick(j));
				entryWritable.setValR(rOldVector.get(j));
				context.write(new IntWritable(j), entryWritable);
			}*/
		}

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			Configuration config = context.getConfiguration();

			colnums = Integer.parseInt(config
					.get(APCParallelUpdateRAJob.COL_NUMS));
			lamda=Double.parseDouble(config.get(APCParallelUpdateRAJob.LAMDA));
		}
	}

	public static class APCParallelUpdateRATransposeMapper
			extends
			Mapper<IntWritable, APCMatrixEntryWritable, IntWritable, APCMatrixEntryWritable> {

		private int colnums;
		private double lamda;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			
			super.setup(context);

			Configuration config = context.getConfiguration();

			colnums = Integer.parseInt(config
					.get(APCParallelUpdateRAJob.COL_NUMS));

		}

		@Override
		protected void map(IntWritable key, APCMatrixEntryWritable value,
				Context ctx) throws IOException, InterruptedException {
			// compute responsibility
			ctx.write(key, value);
		}

	}

	public static class APCParallelUpdateRATransposeReducer
			extends
			Reducer<IntWritable, APCMatrixEntryWritable, IntWritable, APCRowVectorWritable> {
		private int colnums;
		

		@Override
		protected void reduce(IntWritable key,
				Iterable<APCMatrixEntryWritable> values, Context context)
				throws IOException, InterruptedException {
			// compute availiability
			Vector rRowVector = new RandomAccessSparseVector(colnums, 100);
			Vector aVector = new RandomAccessSparseVector(colnums, 100);
			Vector stRowVector = new RandomAccessSparseVector(colnums, 100);
			int rownum = key.get();
			for (APCMatrixEntryWritable element : values) {
				rRowVector.setQuick(element.getCol(), element.getValR());
				aVector.setQuick(element.getCol(), element.getValA());
				stRowVector.setQuick(element.getCol(), element.getValS());
			}

			SequentialAccessSparseVector outputS = new SequentialAccessSparseVector(
					stRowVector);
			SequentialAccessSparseVector outputA = new SequentialAccessSparseVector(
					aVector);
			SequentialAccessSparseVector outputR = new SequentialAccessSparseVector(
					rRowVector);
			APCRowVectorWritable rowVectorWritable = new APCRowVectorWritable(
					outputA, outputR, outputS);
			//System.out.println(key.toString());
			//System.out.println(outputA.toString());
			///System.out.println(outputR.toString());
			//System.out.println(outputS.toString());
			context.write(key, rowVectorWritable);
		}

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			
			super.setup(context);
			Configuration config = context.getConfiguration();

			colnums = Integer.parseInt(config
					.get(APCParallelUpdateRAJob.COL_NUMS));
			
		}
	}
}
