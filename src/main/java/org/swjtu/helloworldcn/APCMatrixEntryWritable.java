/**
 * 
 */
package org.swjtu.helloworldcn;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


/**
 * @author Tang 
 * @since 2012-3
 * 
 * wraper the element value for ap clustering computing
 */
public class APCMatrixEntryWritable implements
		WritableComparable<APCMatrixEntryWritable> {


	private int row; //row
	private int col; //column
	private double valA; //availability value
	private double valR; //responsibility value
	private double valS; //similarly value

	public int getRow() {
		return row;
	}

	public void setRow(int row) {
		this.row = row;
	}

	public int getCol() {
		return col;
	}

	public void setCol(int col) {
		this.col = col;
	}

	public double getValA() {
		return valA;
	}

	public double getValR() {
		return valR;
	}

	public double getValS() {
		return valS;
	}

	public void setValA(double val) {
		this.valA = val;
	}

	public void setValR(double val) {
		this.valR = val;
	}

	public void setValS(double val) {
		this.valS = val;
	}

	@Override
	public int hashCode() {
		return row + 31 * col;
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(row);
		out.writeInt(col);
		out.writeDouble(valA);
		out.writeDouble(valR);
		out.writeDouble(valS);
	}

	public void readFields(DataInput in) throws IOException {
		row = in.readInt();
		col = in.readInt();
		valA = in.readDouble();
		valR = in.readDouble();
		valS = in.readDouble();
	}

	@Override
	public String toString() {
		return "(" + row + ',' + col + "):" + valA + "," + valR + "," + valS;
	}

	/* (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	public int compareTo(APCMatrixEntryWritable o) {
		// TODO Auto-generated method stub
		return 0;
	}

}
