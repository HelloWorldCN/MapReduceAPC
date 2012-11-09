/**
 * 
 */
package org.swjtu.helloworldcn;

import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;

/**
 * @author Tang 
 * @since 2012-3
 * 入口程序
 */
public class APClusteringMain extends AbstractJob {



	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// ToolRunner.run(new SpectralKMeansDriver(), args);
		try {
			ToolRunner.run(new APCDriver(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	

	/**
	 * input data is similary data
	 * 
	 * @param args
	 *            passed from main function
	 */
	@Deprecated
	public void runWithSimilaryData(String[] args) {

	}

	/**
	 * input is original data using list of vector: 2.1,3.4,1.7 0.5,1.5,0.7
	 * 
	 * @param args
	 */
	@Deprecated
	public void runWithOriginalData(String[] args) {

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Deprecated
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		
		
		
		return 0;
	}
}
