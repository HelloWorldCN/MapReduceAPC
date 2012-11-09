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
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/**
 * @author Tang
 * Writable to handle serialization of a APCRowVector
 * 行向量的数据封装类
 */
public final class APCRowVectorWritable implements Writable {

	private final VectorWritable A = new VectorWritable();
	private final VectorWritable S = new VectorWritable();
	private final VectorWritable R = new VectorWritable();

	public APCRowVectorWritable() {
	}

	public APCRowVectorWritable(Vector a, Vector r, Vector s) {
		this.A.set(a);
		this.R.set(r);
		this.S.set(s);
	}

	public Vector getVectorA() {
		return A.get();
	}

	public Vector getVectorR() {
		return R.get();
	}

	public Vector getVectorS() {
		return S.get();
	}

	public void setVectorA(Vector vector) {
		A.set(vector);
	}

	public void setVectorR(Vector vector) {
		R.set(vector);
	}

	public void setVectorS(Vector vector) {
		S.set(vector);
	}

	public void readFields(DataInput in) {
		try {
			A.readFields(in);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			R.readFields(in);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			S.readFields(in);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void write(DataOutput out) throws IOException {
		A.write(out);
		R.write(out);
		S.write(out);

	}

	public static APCRowVectorWritable read(DataInput in) throws IOException {
		APCRowVectorWritable writable = new APCRowVectorWritable();
		writable.readFields(in);
		return writable;
	}

}
