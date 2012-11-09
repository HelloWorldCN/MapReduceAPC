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
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * Handles reading the files representing the similarly matrix.  Each line in all the files
 * should take the form:
 * {i,j,value}

 * where i and i are the ith and jth data points
 * in the entire set, and value represents some measurement of their
 * relative absolute magnitudes.
 */
public class APCMatrixInputMapper extends
		Mapper<LongWritable, Text, IntWritable, APCMatrixEntryWritable> {

	private static final Logger log = LoggerFactory
			.getLogger(APCMatrixInputMapper.class);

	private static final Pattern COMMA_PATTERN = Pattern.compile(",");
	private int nonoise;
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {

		super.setup(context);
		nonoise=context.getConfiguration().getInt(APCMatrixInputJob.TEMPORARY_SAVE_NONOISE, 0);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] elements = COMMA_PATTERN.split(value.toString());

		// enforce well-formed textual representation of the similarly
		if (elements.length != 3) {
			throw new IOException(
					"Expected input of length 3, received "
							+ elements.length
							+ ". Please make sure you adhere to "
							+ "the structure of (i,j,value) for representing a relation in input data. "
							+ "Input line was: '" + value + "'.");
		}
		if (elements[0].isEmpty() || elements[1].isEmpty()
				|| elements[2].isEmpty()) {
			throw new IOException(
					"Found an element of 0 length. Please be sure you adhere to the structure of "
							+ "(i,j,value) for  representing a relation in input data.");
		}
		

		// parse the line of text
		APCMatrixEntryWritable toAdd = new APCMatrixEntryWritable();
		IntWritable row = new IntWritable(Integer.valueOf(elements[0]));
		toAdd.setRow(-1); // already set as the Reducer's key
		toAdd.setCol(Integer.valueOf(elements[1]));
		double sOldValue = Double.valueOf(elements[2]);
		
		//add noise
		//avoid degenerate solutions by adding a small amount of noise to the
		//input similarities
		if (nonoise==0) {
			sOldValue = sOldValue + (sOldValue * Double.MIN_VALUE + .0000001)
					* Math.random();
		}
		/*double sValue = sOldValue + (sOldValue * Double.MIN_VALUE + .0000001)
				* Math.random();*/

		toAdd.setValS(sOldValue);
		toAdd.setValA(0.0);
		toAdd.setValR(0.0);
		context.write(row, toAdd);

	}
}
