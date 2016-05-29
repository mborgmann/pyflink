/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.examples.java.spatial;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.spatial.ImageInfoWrapper;
import org.apache.flink.api.java.spatial.envi.ImageInputFormat;
import org.apache.flink.api.java.spatial.envi.ImageOutputFormat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;


public class ImageTest {
	private static int dop;
	private static String filePath;
	private static String outputFilePath;

	public static void main(String[] args) throws Exception {
		if (!parseParameters(args)) {
			return;
		}

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(dop);

		// [5-7] Loading an Image Collection
		DataSet<Tuple3<String, byte[], byte[]>> scenes = readImages(env);
		// [8] Filter by Date
		// scenes = scenes.filter(new FilterByDate("2000-04-01", "2000-04-31"));
		// [9] Filter by RegionOfInterest
		// TODO

		// [10-48] BestObservation
		// TODO

		// [49-74] Statistics
		// Filter by BandNames
		DataSet<Tuple3<String, byte[], byte[]>> scenes2000_blue = scenes
				.filter(new FilterByDate("2000-01-01", "2000-12-31"))
				.filter(new FilterByBandNames("band5"));
		// Calculate Statistics:
		// 0p		- aka min
		// DataSet<Tuple3<String, byte[], byte[]>> scenes2000_blue_min = scenes2000_blue.reduce(new Min());
		// TODO 25p
		// TODO 50p		- aka median
		// TODO 75p
		// 100p	- aka max
		// DataSet<Tuple3<String, byte[], byte[]>> scenes2000_blue_max = scenes2000_blue.reduce(new Max());
		// mean
		//DataSet<Tuple3<String, byte[], byte[]>> scenes2000_blue_avg = scenes2000_blue
		//		.map(new AverageMap())
		//		.reduce(new AverageReduce())
		//		.map(new AverageFinish());
		// stdev
		DataSet<Tuple3<String, byte[], byte[]>> scenes2000_blue_stddev = scenes2000_blue
				.map(new StdDevMap())
				.reduce(new StdDevReduce())
				.map(new StdDevFinish());

		// TODO Put all statistics in one Tuple as different bands

		// [75-95] Classifying
		// TODO

		DataSet<Tuple3<String, byte[], byte[]>> result = scenes2000_blue_stddev;

		ImageOutputFormat outputFormat = new ImageOutputFormat();
		outputFormat.setWriteMode(FileSystem.WriteMode.OVERWRITE);
		result.write(outputFormat, outputFilePath).setParallelism(1);

		env.execute("Image Test");
	}

	private static boolean parseParameters(String[] params) {
		if (params.length > 0) {
			if (params.length != 3) {
				System.out.println("Usage: <dop> <input directory> <output path>");
				return false;
			} else {
				dop = Integer.parseInt(params[0]);
				filePath = params[1];
				outputFilePath = params[2];
			}
		} else {
			System.out.println("Usage: <dop> <input directory> <output path>");
			return false;
		}

		return true;
	}

	private static DataSet<Tuple3<String, byte[], byte[]>> readImages(ExecutionEnvironment env) {
		ImageInputFormat imageFormat = new ImageInputFormat(new Path(filePath));
		imageFormat.configure(true);
		TupleTypeInfo<Tuple3<String, byte[], byte[]>> typeInfo = new TupleTypeInfo<Tuple3<String, byte[], byte[]>>(BasicTypeInfo.STRING_TYPE_INFO, PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO, PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO);
		return new DataSource<Tuple3<String, byte[], byte[]>>(env, imageFormat, typeInfo, "imageSource");
	}

}

/**
 * Filters by AcquisitionDate
 */
class FilterByDate implements FilterFunction<Tuple3<String, byte[], byte[]>> {

	private Date oldest;
	private Date youngest;

	/**
	 * @param oldest String-Format: "yyyy-MM-dd"
	 * @param youngest String-Format: "yyyy-MM-dd"
	 * @throws ParseException
     */
	public FilterByDate(String oldest, String youngest) throws ParseException {
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd");

		if (oldest != null) {
			this.oldest = df.parse(oldest);
		}
		if (youngest != null) {
			this.youngest = df.parse(youngest);
		}
	}


	@Override
	public boolean filter(Tuple3<String, byte[], byte[]> value) throws Exception {

		ImageInfoWrapper metaData = new ImageInfoWrapper(value.f1);
		boolean in = true;

		if (in && youngest != null) {
			in = metaData.getAcquisitionDateAsLong() <= youngest.getTime();
		}

		if (in && oldest != null) {
			in = metaData.getAcquisitionDateAsLong() >= oldest.getTime();
		}

		return in;
	}
}

/**
 * Filters by BandNames
 */
class FilterByBandNames implements FilterFunction<Tuple3<String, byte[], byte[]>> {

	private String[] bandNames;

	public FilterByBandNames(String... bandNames) {
		this.bandNames = bandNames;
	}

	@Override
	public boolean filter(Tuple3<String, byte[], byte[]> value) throws Exception {

		ImageInfoWrapper metaData = new ImageInfoWrapper(value.f1);
		// Assuming there is only one band in the input
		String band = metaData.getBandNames()[0];

		if (Arrays.asList(this.bandNames).contains(band))  {
			return true;
		}
		return false;
	}
}

/**
 * Implementation follows: https://de.wikipedia.org/wiki/Standardabweichung#Berechnung_f.C3.BCr_auflaufende_Messwerte
 * Might be replaced with better algorithm from Knuth - SemiNumericalAlgorithms(4.2.2) or Welford - Technometrics
 *
 * Tuple: Key, Header, valueCount, SquareSums, Sums
 */
class StdDevMap implements  MapFunction<Tuple3<String, byte[], byte[]>, Tuple5<String, byte[], int[], long[], int[]>> {

	@Override
	public Tuple5<String, byte[], int[], long[], int[]> map(Tuple3<String, byte[], byte[]> value) throws Exception {
		ImageInfoWrapper metaData = new ImageInfoWrapper(value.f1);
		short dataIgnoreValue = metaData.getDataIgnoreValue();

		Tuple5<String, byte[], int[], long[], int[]> result = new Tuple5<>();
		result.f0 = value.f0;
		result.f1 = value.f1;

		short[] values = ArrayHelper.byteToShort(value.f2);
		int[] valueCount = new int[values.length];
		long[] squares = new long[values.length];
		int[] sums = new int[values.length];

		for(int i = 0; i < sums.length; i++) {
			if (values[i] != dataIgnoreValue) {
				sums[i] = values[i];
				squares[i] = values[i] * values[i];
				valueCount[i] = 1;
			} else {
				sums[i] = 0;
				squares[i] = 0;
				valueCount[i] = 0;
			}
		}

		result.f2 = valueCount;
		result.f3 = squares;
		result.f4 = sums;

		return result;
	}
}

class StdDevReduce implements ReduceFunction<Tuple5<String, byte[], int[], long[], int[]>> {

	@Override
	public Tuple5<String, byte[], int[], long[], int[]> reduce(Tuple5<String, byte[], int[], long[], int[]> value1, Tuple5<String, byte[], int[], long[], int[]> value2) throws Exception {
		ImageInfoWrapper metaDataLeft = new ImageInfoWrapper(value1.f1);
		ImageInfoWrapper metaDataRight = new ImageInfoWrapper(value2.f1);
		short dataIgnoreValue = metaDataLeft.getDataIgnoreValue();

		int samplesLeft = metaDataLeft.getSamples();
		int samplesRight = metaDataRight.getSamples();
		int samplesOut = samplesLeft > samplesRight ? samplesLeft : samplesRight;

		int linesLeft = metaDataLeft.getLines();
		int linesRight = metaDataRight.getLines();
		int linesOut = linesLeft > linesRight ? linesLeft : linesRight;

		// TODO Some sanity checks?

		// Convert input to shorts (assuming only datatype ever used)
		int[] counterLeft = value1.f2;
		long[] squaresLeft = value1.f3;
		int[] sumsLeft = value1.f4;
		int[] counterRight = value2.f2;
		long[] squaresRight = value2.f3;
		int[] sumsRight = value2.f4;

		int[] counter = new int[linesOut * samplesOut];
		int[] sums = new int[linesOut * samplesOut];
		long[] squares = new long[linesOut * samplesOut];

		ImageInfoWrapper metaDataMin = metaDataLeft;
		metaDataMin.setBandNames(new String[]{"std-dev"});
		metaDataMin.setLines(linesOut);
		metaDataMin.setSamples(samplesOut);

		for(int i = 0; i < linesOut; i++) {
			for(short j = 0; j < samplesOut; j++) {
				// Set default
				int outPos = i * samplesOut + j;

				// Load data, if available
				int countLeft = 0;
				long squareLeft = 0;
				int sumLeft = 0;
				if (i < linesLeft && j < samplesLeft) {
					int pos = i * samplesLeft + j;

					countLeft = counterLeft[pos];
					squareLeft = squaresLeft[pos];
					sumLeft = sumsLeft[pos];
				}

				int countRight = 0;
				long squareRight = 0;
				int sumRight = 0;
				if (i < linesRight && j < samplesRight) {
					int pos = i * samplesRight + j;

					countRight = counterRight[pos];
					squareRight = squaresRight[pos];
					sumRight = sumsRight[pos];
				}

				counter[outPos] = countLeft + countRight;
				squares[outPos] = squareLeft + squareRight;
				sums[outPos] = sumLeft + sumRight;
			}
		}

		return new Tuple5<>(value1.f0, metaDataMin.toBytes(), counter, squares, sums);
	}
}

class StdDevFinish implements MapFunction<Tuple5<String, byte[], int[], long[], int[]>, Tuple3<String, byte[], byte[]>> {

	@Override
	public Tuple3<String, byte[], byte[]> map(Tuple5<String, byte[], int[], long[], int[]> value) throws Exception {
		ImageInfoWrapper metaData = new ImageInfoWrapper(value.f1);
		short dataIgnoreValue = metaData.getDataIgnoreValue();

		Tuple3<String, byte[], byte[]> result = new Tuple3<>();

		result.f0 = value.f0;
		result.f1 = value.f1;

		int[] counts = value.f2;
		long[] squares = value.f3;
		int[] sums = value.f4;

		short[] stdDevs = new short[sums.length];

		for (int i = 0; i < stdDevs.length; i++) {
			if (counts[i] > 0) {

				double a = 0;
				if (counts[i] > 1) {
					a = (squares[i] - (sums[i] * sums[i]) / counts[i]) / (counts[i] - 1);
				}
				if (a != 0) {
					a = Math.sqrt(a);
				}

				stdDevs[i] = (short) a;
			} else  {
				stdDevs[i] = dataIgnoreValue;
			}
		}

		result.f2 = ArrayHelper.shortToByte(stdDevs);

		return result;
	}
}

class AverageMap implements MapFunction<Tuple3<String, byte[], byte[]>, Tuple4<String, byte[], int[], int[]>> {

	@Override
	public Tuple4<String, byte[], int[], int[]> map(Tuple3<String, byte[], byte[]> value) throws Exception {
		ImageInfoWrapper metaData = new ImageInfoWrapper(value.f1);
		short dataIgnoreValue = metaData.getDataIgnoreValue();


		Tuple4<String, byte[], int[], int[]> result = new Tuple4<String, byte[], int[], int[]>();
		result.f0 = value.f0;
		result.f1 = value.f1;

		short[] values = ArrayHelper.byteToShort(value.f2);
		int[] valueCount = new int[values.length];
		int[] sums = new int[values.length];

		for(int i = 0; i < sums.length; i++) {
			if (values[i] != dataIgnoreValue) {
				sums[i] = values[i];
				valueCount[i] = 1;
			} else {
				sums[i] = 0;
				valueCount[i] = 0;
			}
		}

		result.f2 = valueCount;
		result.f3 = sums;
		return result;
	}
}

class AverageReduce implements ReduceFunction<Tuple4<String, byte[], int[], int[]>> {

	@Override
	public Tuple4<String, byte[], int[], int[]> reduce(Tuple4<String, byte[], int[], int[]> value1, Tuple4<String, byte[], int[], int[]> value2) throws Exception {
		ImageInfoWrapper metaDataLeft = new ImageInfoWrapper(value1.f1);
		ImageInfoWrapper metaDataRight = new ImageInfoWrapper(value2.f1);
		short dataIgnoreValue = metaDataLeft.getDataIgnoreValue();

		int samplesLeft = metaDataLeft.getSamples();
		int samplesRight = metaDataRight.getSamples();
		int samplesOut = samplesLeft > samplesRight ? samplesLeft : samplesRight;

		int linesLeft = metaDataLeft.getLines();
		int linesRight = metaDataRight.getLines();
		int linesOut = linesLeft > linesRight ? linesLeft : linesRight;

		// TODO Some sanity checks?

		// Convert input to shorts (assuming only datatype ever used)
		int[] valuesLeft = value1.f3;
		int[] counterLeft = value1.f2;
		int[] valuesRight = value2.f3;
		int[] counterRight = value2.f2;
		int[] counter = new int[linesOut * samplesOut];
		int[] sums = new int[linesOut * samplesOut];

		ImageInfoWrapper metaDataMin = metaDataLeft;
		metaDataMin.setBandNames(new String[]{"average"});
		metaDataMin.setLines(linesOut);
		metaDataMin.setSamples(samplesOut);

		for(int i = 0; i < linesOut; i++) {
			for(short j = 0; j < samplesOut; j++) {
				// Set default
				int outPos = i * samplesOut + j;

				// Load data, if available
				int valueLeft = 0;
				int countLeft = 0;
				if (i < linesLeft && j < samplesLeft) {
					int pos = i * samplesLeft + j;
					valueLeft = valuesLeft[pos];
					countLeft = counterLeft[pos];
				}

				int valueRight = 0;
				int countRight = 0;
				if (i < linesRight && j < samplesRight) {
					int pos = i * samplesRight + j;
					valueRight = valuesRight[pos];
					countRight = counterRight[pos];
				}

				counter[outPos] = countLeft + countRight;
				sums[outPos] = valueLeft + valueRight;
			}
		}

		return new Tuple4<>(value1.f0, metaDataMin.toBytes(), counter, sums);
	}
}

class AverageFinish implements MapFunction<Tuple4<String, byte[], int[], int[]>, Tuple3<String, byte[], byte[]>> {

	@Override
	public Tuple3<String, byte[], byte[]> map(Tuple4<String, byte[], int[], int[]> value) throws Exception {
		ImageInfoWrapper metaData = new ImageInfoWrapper(value.f1);
		short dataIgnoreValue = metaData.getDataIgnoreValue();

		Tuple3<String, byte[], byte[]> result = new Tuple3<>();

		result.f0 = value.f0;
		result.f1 = value.f1;

		int[] counts = value.f2;
		int[] sums = value.f3;
		short[] averages = new short[sums.length];

		for (int i = 0; i < averages.length; i++) {
			if (counts[i] > 0) {
				averages[i] = (short) (sums[i] / counts[i]);
			} else  {
				averages[i] = dataIgnoreValue;
			}
		}

		result.f2 = ArrayHelper.shortToByte(averages);

		return result;
	}
}

/**
 * Returns the min for each pixel, sets bandName "Min"
 */
class Min implements ReduceFunction<Tuple3<String, byte[], byte[]>> {

	@Override
	public Tuple3<String, byte[], byte[]> reduce(Tuple3<String, byte[], byte[]> value1, Tuple3<String, byte[], byte[]> value2) throws Exception {
		ImageInfoWrapper metaDataLeft = new ImageInfoWrapper(value1.f1);
		ImageInfoWrapper metaDataRight = new ImageInfoWrapper(value2.f1);
		short dataIgnoreValue = metaDataLeft.getDataIgnoreValue();

		int samplesLeft = metaDataLeft.getSamples();
		int samplesRight = metaDataRight.getSamples();
		int samplesOut = samplesLeft > samplesRight ? samplesLeft : samplesRight;

		int linesLeft = metaDataLeft.getLines();
		int linesRight = metaDataRight.getLines();
		int linesOut = linesLeft > linesRight ? linesLeft : linesRight;

		// TODO Some sanity checks?

		// Convert input to shorts (assuming only datatype ever used)
		short[] valuesLeft = ArrayHelper.byteToShort(value1.f2);
		short[] valuesRight = ArrayHelper.byteToShort(value2.f2);
		short[] min = new short[linesOut * samplesOut];

		ImageInfoWrapper metaDataMin = metaDataLeft;
		metaDataMin.setBandNames(new String[]{"min"});
		metaDataMin.setLines(linesOut);
		metaDataMin.setSamples(samplesOut);

		for(int i = 0; i < linesOut; i++) {
			for(short j = 0; j < samplesOut; j++) {
				// Set default
				int outPost = i * samplesOut + j;
				min[outPost] = dataIgnoreValue;

				// Load data, if available
				short valueLeft = dataIgnoreValue;
				if (i < linesLeft && j < samplesLeft) {
					int pos = i * samplesLeft + j;
					valueLeft = valuesLeft[pos];
				}

				short valueRight = dataIgnoreValue;
				if (i < linesRight && j < samplesRight) {
					int pos = i * samplesRight + j;
					valueRight = valuesRight[pos];
				}

				// Find min
				if (valueLeft == dataIgnoreValue && valueRight != dataIgnoreValue) {
					min[outPost] = valueRight;
				}
				if (valueLeft != dataIgnoreValue && valueRight == dataIgnoreValue) {
					min[outPost] = valueLeft;
				}
				if (valueLeft != dataIgnoreValue && valueRight != dataIgnoreValue) {
					min[outPost] = valueLeft < valueRight ? valueLeft : valueRight;
				}
			}
		}

		return new Tuple3<>(value1.f0, metaDataMin.toBytes(), ArrayHelper.shortToByte(min));
	}
}

/**
 * Returns the max for each pixel, sets bandName "Max"
 */
class Max implements ReduceFunction<Tuple3<String, byte[], byte[]>> {

	@Override
	public Tuple3<String, byte[], byte[]> reduce(Tuple3<String, byte[], byte[]> value1, Tuple3<String, byte[], byte[]> value2) throws Exception {
		ImageInfoWrapper metaDataLeft = new ImageInfoWrapper(value1.f1);
		ImageInfoWrapper metaDataRight = new ImageInfoWrapper(value2.f1);
		short dataIgnoreValue = metaDataLeft.getDataIgnoreValue();

		int samplesLeft = metaDataLeft.getSamples();
		int samplesRight = metaDataRight.getSamples();
		int samplesOut = samplesLeft > samplesRight ? samplesLeft : samplesRight;

		int linesLeft = metaDataLeft.getLines();
		int linesRight = metaDataRight.getLines();
		int linesOut = linesLeft > linesRight ? linesLeft : linesRight;

		// TODO Some sanity checks?

		// Convert input to shorts (assuming only datatype ever used)
		short[] valuesLeft = ArrayHelper.byteToShort(value1.f2);
		short[] valuesRight = ArrayHelper.byteToShort(value2.f2);
		short[] max = new short[linesOut * samplesOut];

		ImageInfoWrapper metaDataMin = metaDataLeft;
		metaDataMin.setBandNames(new String[]{"max"});
		metaDataMin.setLines(linesOut);
		metaDataMin.setSamples(samplesOut);

		for(int i = 0; i < linesOut; i++) {
			for(short j = 0; j < samplesOut; j++) {
				// Set default
				int outPost = i * samplesOut + j;
				max[outPost] = dataIgnoreValue;

				// Load data, if available
				short valueLeft = dataIgnoreValue;
				if (i < linesLeft && j < samplesLeft) {
					int pos = i * samplesLeft + j;
					valueLeft = valuesLeft[pos];
				}

				short valueRight = dataIgnoreValue;
				if (i < linesRight && j < samplesRight) {
					int pos = i * samplesRight + j;
					valueRight = valuesRight[pos];
				}

				// Find min
				if (valueLeft == dataIgnoreValue && valueRight != dataIgnoreValue) {
					max[outPost] = valueRight;
				}
				if (valueLeft != dataIgnoreValue && valueRight == dataIgnoreValue) {
					max[outPost] = valueLeft;
				}
				if (valueLeft != dataIgnoreValue && valueRight != dataIgnoreValue) {
					max[outPost] = valueLeft > valueRight ? valueLeft : valueRight;
				}
			}
		}

		return new Tuple3<>(value1.f0, metaDataMin.toBytes(), ArrayHelper.shortToByte(max));
	}
}

class ArrayHelper {

	public static short[] byteToShort(byte[] byteArray) {
		short[] shorts = new short[byteArray.length / 2];
		ByteBuffer.wrap(byteArray).order(ByteOrder.LITTLE_ENDIAN).asShortBuffer().get(shorts);
		return shorts;
	}

	public static byte[] shortToByte(short[] shortArray) {
		byte[] bytes = new byte[shortArray.length * 2];
		ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).asShortBuffer().put(shortArray);
		return bytes;
	}

}