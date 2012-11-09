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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * @author Tang 
 * This class is a Writable implementation of apc element value
 * 简单的键值对数据封装
 */
public class APCKeyValuePairWritable implements Writable {

	private int key;
	private double valueA;
	private double valueR;
	private double valueS;

	public APCKeyValuePairWritable() {
	}

	public APCKeyValuePairWritable(int k, double a, double r, double s) {
		this.key = k;
		this.valueA = a;
		this.valueR = r;
		this.valueS = s;
	}

	public void setKey(int k) {
		this.key = k;
	}

	public void setValueA(double v) {
		this.valueA = v;
	}

	public void setValueR(double v) {
		this.valueR = v;
	}

	public void setValueS(double v) {
		this.valueS = v;
	}

	public void readFields(DataInput in) throws IOException {
		this.key = in.readInt();
		this.valueA = in.readDouble();
		this.valueR = in.readDouble();
		this.valueS = in.readDouble();
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(key);
		out.writeDouble(valueA);
		out.writeDouble(valueR);
		out.writeDouble(valueS);
	}

	public int getKey() {
		return key;
	}

	public double getValueA() {
		return valueA;
	}

	public double getValueR() {
		return valueR;
	}

	public double getValueS() {
		return valueS;
	}
}
