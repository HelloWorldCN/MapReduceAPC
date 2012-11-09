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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.ClassUtils;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is the job's controller of ap clustering 
 * @author tang
 * @since 2012-3
 */
public class APCDriver extends AbstractJob {
	private static final Logger log = LoggerFactory.getLogger(APCDriver.class);
	private Boolean unconverged = true;
	Boolean symmetric;

	//DistributedRowMatrix e;
	int numDims;
	int maxIterations;
	int convits;
	double lamda;
	int nonoise;
	ArrayList<Integer> exemplars;
	Vector clusteringResult;
	ArrayList<RandomAccessSparseVector> e;

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = getConf();
		addInputOption();
		addOutputOption();
		addOption("dimensions", "d", "Square dimensions of affinity matrix",
				true);
		addOption(DefaultOptionCreator.distanceMeasureOption().create());
		addOption(DefaultOptionCreator.maxIterationsOption().create());
		addOption(
				"inputdatatype",
				"tp",
				"Input data type,0 indicate original data; 1 indicate similary data",
				"0");

		addOption(
				"dampfact",
				"lam",
				"update equation damping level in [0.5, 1).  Higher  values correspond to heavy damping, which may be needed if oscillations occur. (default: 0.9)",
				"0.9");
		addOption(
				"convits",
				"cv",
				"if the estimated exemplars stay fixed for convits iterations, APCLUSTER terminates early (default: 100)",
				"100");
		// preference
		addOption(
				"preference",
				"pf",
				"indicates the preference that data point i be chosen as an exemplar",
				"100");
		addOption(
				"nonoise",
				"nn",
				"APCLUSTER adds a small amount of noise to similary data to prevent degenerate cases:(default: 0 enabled;1 disabled)",
				"0");
		Map<String, String> parsedArgs = parseArguments(arg0);
		if (parsedArgs == null) {
			return 0;
		}

		Path input = getInputPath();
		Path output = getOutputPath();
		// if (hasOption(DefaultOptionCreator.OVERWRITE_OPTION)) {
		HadoopUtil.delete(conf, output);
		// }
		numDims = Integer.parseInt(parsedArgs.get("--dimensions"));
		String measureClass = getOption(DefaultOptionCreator.DISTANCE_MEASURE_OPTION);
		DistanceMeasure measure = ClassUtils.instantiateAs(measureClass,
				DistanceMeasure.class);

		maxIterations = Integer
				.parseInt(getOption(DefaultOptionCreator.MAX_ITERATIONS_OPTION));
		System.out.println("convits"+parsedArgs.get("--convits"));
		convits = Integer.parseInt(parsedArgs.get("--convits"));
		lamda = Double.parseDouble(parsedArgs.get("--dampfact"));
		nonoise=Integer.parseInt(parsedArgs.get("--nonoise"));
		double preference = Double.parseDouble(parsedArgs.get("--preference"));
		int inputDataType=Integer.parseInt(parsedArgs.get("--inputdatatype"));
		e=new ArrayList<RandomAccessSparseVector>(convits);
		run(conf, input, output, numDims, preference,inputDataType, measure, lamda,
				maxIterations);

		return 0;
	}

	/**
	 * Run the ap clustering on the supplied arguments
	 * 
	 * @param conf
	 *            the Configuration to be used
	 * @param input
	 *            the Path to the input tuples directory
	 * @param output
	 *            the Path to the output directory
	 * @param numDims
	 *            the number of dimensions of the similarly matrix
	 * @param preference
	 *            indicates the preference that data point i be chosen as an
	 *            exemplar
	 * @param convits
	 *            if the estimated exemplars stay fixed for convits iterations,
	 *            APCLUSTER terminates early
	 * @param measure
	 *            the DistanceMeasure for the clustering calculations,currently not used!
	 * @param lamda
	 *            update equation damping level in [0.5, 1). Higher values
	 *            correspond to heavy damping, which may be needed if
	 *            oscillations occur. (default: 0.9)
	 * @param maxIterations
	 *            the int maximum number of iterations for the k-Means
	 *            calculations
	 */
	public void run(Configuration conf, Path input, Path output, int numDims,
			 double preference, int inputDataType,DistanceMeasure measure,
			double lamda, int maxIterations) throws IOException,
			InterruptedException, ClassNotFoundException {
		// create a few new Paths for temp files and transformations
		Path outputCalc = new Path(output, "calculations");
		Path outputTmp = new Path(output, "temporary");
		unconverged = true;
		/*if (inputDataType==0) {
			//compute similary
			
		}*/
	    if (preference==0) {
	    	preference=APCGetMatrixPreference.runJob(input,output);
		}


		// Take in the raw CSV text file and split it ourselves,
		// creating our own SequenceFiles for the matrices to read later
		// (similar to the style of syntheticcontrol.canopy.InputMapper)
		Path affSeqFiles = new Path(outputCalc, "similaryseqfile-"
				+ (System.nanoTime() & 0xFF));
		// Load similary data
		APCMatrixInputJob.runJob(input, affSeqFiles, numDims, numDims,preference,nonoise);
		// Path outputPath = FileOutputFormat.getOutputPath(new JobConf(conf));

		// Next step: construct the affinity matrix using the newly-created
		// sequence files
		/*
		st = new DistributedRowMatrix(affSeqFiles, new Path(outputTmp,
				"similarytmp-" + (System.nanoTime() & 0xFF)), numDims, numDims);
		symmetric = true;
		if (!symmetric) {
			st = st.transpose();
		}
		Configuration depConf = new Configuration(conf);
		st.setConf(depConf);
		Path ASeqFiles = new Path(outputCalc, "Avaseqfile-"
				+ (System.nanoTime() & 0xFF));
		A = new DistributedRowMatrix(ASeqFiles, new Path(outputTmp, "Avatmp-"
				+ (System.nanoTime() & 0xFF)), numDims, numDims);
		A.setConf(depConf);

		Path RSeqFiles = new Path(outputCalc, "Resfile-"
				+ (System.nanoTime() & 0xFF));
		R = new DistributedRowMatrix(RSeqFiles, new Path(outputTmp, "Restmp-"
				+ (System.nanoTime() & 0xFF)), numDims, numDims);
		R.setConf(depConf);
		*/
		/*Configuration depConf = new Configuration(conf);
		Path eSeqFiles = new Path(outputCalc, "exfile-"
				+ (System.nanoTime() & 0xFF));
		e = new DistributedRowMatrix(eSeqFiles, new Path(outputTmp, "extmp-"
				+ (System.nanoTime() & 0xFF)), numDims, convits);
		e.setConf(depConf);*/
		

		//initARData(conf);
		parallelUpdateRA(conf, affSeqFiles,outputCalc);
		printClusteringResult();

	}

	@Deprecated
	private void initARData(Configuration conf) throws IOException {
		/*FileSystem fs = FileSystem.get(conf);
		RandomAccessSparseVector out = new RandomAccessSparseVector(
				A.numCols(), 100);
		SequentialAccessSparseVector outputVector = new SequentialAccessSparseVector(
				out);
		IntWritable rowkey = new IntWritable();
		VectorWritable valWritable = new VectorWritable(outputVector);
		SequenceFile.Writer writer1 = SequenceFile.createWriter(fs, conf,
				new Path(R.getRowPath(), "1"), rowkey.getClass(),
				valWritable.getClass());
		SequenceFile.Writer writer2 = SequenceFile.createWriter(fs, conf,
				new Path(A.getRowPath(), "1"), rowkey.getClass(),
				valWritable.getClass());
		for (int j = 0; j < A.numCols(); j++) {
			rowkey.set(j);
			writer1.append(rowkey, valWritable);
			writer2.append(rowkey, valWritable);
		}
		IOUtils.closeStream(writer1);
		IOUtils.closeStream(writer2);*/
	}

	/**
	 * @param conf
	 * 		the Configuration to be used
	 * @param inputPath
	 * 		the Path to the input tuples directory
	 * @param outputCalc
	 * 		the Path to the output directory
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	private void parallelUpdateRA(Configuration conf, Path inputPath,Path outputCalc)
			throws IOException, ClassNotFoundException, InterruptedException {

		
		Boolean dn = false;
		FileSystem fs = FileSystem.get(conf);


		int i = -1; 
		Vector diagAplusR=null;
		while (!dn) {
			i = i + 1;
			Path outputPath = new Path(outputCalc, "parallelUpdateRA-"
					+ i);
			if (i>1) {
				HadoopUtil.delete(conf, new Path(outputCalc, "parallelUpdateRA-"
						+ (i-2)));
			}
			inputPath=APCParallelUpdateRAJob.runJob(inputPath, outputPath,  numDims,lamda);
			
			diagAplusR=APCGetDiagAplusRJob.runJob(inputPath, numDims);
			System.out.println("diag"+diagAplusR);
			RandomAccessSparseVector E=new RandomAccessSparseVector(numDims, 100);
			//RandomAccessSparseVector E = new RandomAccessSparseVector(diagAplusR);
			Iterator<Element> iter = diagAplusR.iterateNonZero();
			int K=0;
			while (iter.hasNext()) {
				Element element = iter.next();
				if (element.get() > 0) {
					E.set(element.index(), 1.0);
					K++;
				} else {
					E.set(element.index(), 0.0);
				}				
			}
			
			/*for (int c = 0; c < E.size(); c++) {
				if (E.get(c) > 0) {
					E.set(c, 1.0);
				} else {
					E.set(c, 0.0);
				}
			}*/
			
			//System.out.println("con is"+convits);
			int index = i % convits;
			if (i>=convits) {
				e.remove(index);
			}
			
			e.add(index, E);
			System.out.println(e);
			

			//double K = E.zSum();
			if (i >= convits || i >= maxIterations) {
				RandomAccessSparseVector se = new RandomAccessSparseVector(
						numDims, 100);
				Iterator<RandomAccessSparseVector> iterator = e.iterator();
				while (iterator.hasNext()) {
					RandomAccessSparseVector v = iterator.next();
					se=(RandomAccessSparseVector) se.plus(v);
				}
				System.out.println("se"+se);
				iter = se.iterateNonZero();
				while (iter.hasNext()) {
					Element element = iter.next();
					if (element.get() != convits) {
						unconverged = true;
						break;
					} else {
						unconverged = false;
					}
				}
				if ((!unconverged && K > 0) || i == maxIterations) {
					dn = true;
				}

			}
		}	
        //Get exemplars     
		exemplars= new ArrayList<Integer>();
		Path outputExemplasPath=new Path(new Path(outputCalc, "exemplars"), "result");
		IntWritable exValue=new IntWritable();
		SequenceFile.Writer writerExemplas = SequenceFile.createWriter(fs, conf,
				outputExemplasPath, NullWritable.get().getClass(), exValue.getClass());
		Iterator<Element> iter = diagAplusR.iterateNonZero();
		while (iter.hasNext()) {
			Element element = iter.next();
			if (element.get() > 0) {
                exemplars.add(element.index());
                exValue.set(element.index());
                writerExemplas.append(NullWritable.get(),exValue );
            }
		}
        /*for (int k = 0; k < numDims; k++) {
            if (diagAplusR.getQuick(k) > 0) {
                exemplars.add(k);
                exValue.set(k);
                writerExemplas.append(NullWritable.get(),exValue );
            }
        }*/
        writerExemplas.close();
        
        clusteringResult=APCGetClusteringResultJob.runJob(inputPath, outputExemplasPath, numDims);
        /*for (int j = 0; j< exemplars.size(); j++) {
        	clusteringResult.set(exemplars.get(j), exemplars.get(j));          
        }*/
        //System.out.println("julei jieguo:"+clusteringResult);
	}
	
	public void printClusteringResult(){
		System.out.println("...................APC Clustering Result...................");
		System.out.println("Total data points is: " +numDims);
		System.out.println("Number of clusers: "+exemplars.size());
		System.out.println("===================Clustering Result Detail===================");
		System.out.println("Output Format:Data point ID,Exemplars");
		for (int i = 0; i < clusteringResult.size(); i++) {		
			System.out.println(i+","+(int)clusteringResult.get(i));
		}	
	}
}
