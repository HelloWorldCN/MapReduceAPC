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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author Tang
 * @since 2012-3
 */
public class APCMatrixInputReducer
		extends
		Reducer<IntWritable, APCMatrixEntryWritable, IntWritable, APCRowVectorWritable> {

	private static final Logger log = LoggerFactory
			.getLogger(APCMatrixInputReducer.class);
	private Double preference=null;


	@Override
	protected void reduce(IntWritable row,
			Iterable<APCMatrixEntryWritable> values, Context context)
			throws IOException, InterruptedException {
		int size = context.getConfiguration().getInt(
				APCMatrixInputJob.MATRIX_DIMENSIONS, Integer.MAX_VALUE);
		RandomAccessSparseVector outS = new RandomAccessSparseVector(size, 100);
		RandomAccessSparseVector outA = new RandomAccessSparseVector(size, 100);
		RandomAccessSparseVector outR = new RandomAccessSparseVector(size, 100);
		
		Configuration conf=context.getConfiguration();
		if (preference==null) {
			preference=Double.parseDouble(conf.get(APCMatrixInputJob.TEMPORARY_SAVE_PREFERENCE));
		}
		
		//System.out.println("pian du"+preference);

		for (APCMatrixEntryWritable element : values) {
			outS.setQuick(element.getCol(), element.getValS());
			outA.setQuick(element.getCol(), 0.0);
			outR.setQuick(element.getCol(), 0.0);

		}
		//Place preferences on the diagonal of S
		outS.setQuick(row.get(), preference);
	
		

		SequentialAccessSparseVector outputS = new SequentialAccessSparseVector(
				outS);
		SequentialAccessSparseVector outputA = new SequentialAccessSparseVector(
				outA);
		SequentialAccessSparseVector outputR = new SequentialAccessSparseVector(
				outR);
		APCRowVectorWritable rowVectorWritable = new APCRowVectorWritable(
				outputA, outputR, outputS);
		//System.out.println(outputS);
		context.write(row, rowVectorWritable);
	}
}
