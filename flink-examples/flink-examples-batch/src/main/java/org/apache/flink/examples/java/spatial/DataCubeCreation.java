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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.spatial.Coordinate;
import org.apache.flink.api.java.spatial.TileTimeKeySelector;
import org.apache.flink.api.java.spatial.envi.ImageOutputFormat;
import org.apache.flink.api.java.spatial.envi.TileInputFormat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.fs.Path;

//import org.apache.flink.api.java.io.EnviReader;

/**
 * Example to select a tile from a time series of scenes and to create a cubic
 * representation of it.
 * 1 file:///opt3/gms_sample/ 445404.0572 3135036.4653 1000 30 file:///opt3/gms_sample/out
 * 1 file:///opt3/gms_sample/ 535404.0572 3026556.4653 1000 30 file:///opt3/gms_sample/out
 * 4 hdfs://localhost:50041/geo 535404.0572 3030036.4653 1000 30 hdfs://localhost:50041/out
 * @author Mathias Peters <mathias.peters@informatik.hu-berlin.de>
 *
 */
public class DataCubeCreation {

	private static int dop;
	private static String filePath;
	private static Coordinate leftUpper, rightLower;
	private static int blockSize; // squared blocks for the beginning
	private static String outputFilePath;
	private static int pixelSize;

	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		final ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();
		env.setParallelism(dop);
		
		DataSet<Tuple3<String, byte[], byte[]>> tiles = readTiles(env);
		DataSet<Tuple3<String, byte[], byte[]>> stitchedTimeSlices = tiles.groupBy(
				new TileTimeKeySelector<Tuple3<String, byte[], byte[]>>()).reduceGroup(
				new TileStitchReduce().configure(leftUpper, rightLower,
						blockSize, blockSize));
		DataSink<Tuple3<String, byte[], byte[]>> writeAsEnvi = stitchedTimeSlices.write(new ImageOutputFormat(), outputFilePath);
		
		writeAsEnvi.setParallelism(1);
			
		env.execute("Data Cube Creation");
	}

	private static boolean parseParameters(String[] params) {

		if (params.length > 0) {
			if (params.length != 7) {
				System.out
						.println("Usage: <dop> <input directory> <left-upper-longitude> <left-upper-latitude> <block size> <pixel size> <output path>");
				return false;
			} else {
				dop = Integer.parseInt(params[0]);
				filePath = params[1];
				String leftLong = params[2];
				String leftLat = params[3];
				leftUpper = new Coordinate(Double.parseDouble(leftLong),
						Double.parseDouble(leftLat));

				
				blockSize = Integer.parseInt(params[4]);
				pixelSize = Integer.parseInt(params[5]);
				
				double rightLong = Double.parseDouble(leftLong) + blockSize * pixelSize;
				double rightLat = Double.parseDouble(leftLat) - blockSize * pixelSize;
				
				
				rightLower = new Coordinate(rightLong, rightLat);

				outputFilePath = params[6];
			}
		} else {
			System.out
					.println("Usage: <input directory> <left-upper-longitude>  <left-upper-latitude> <block size> <pixel size> <output path>");
			return false;
		}

		return true;
	}

	private static DataSet<Tuple3<String, byte[], byte[]>> readTiles(ExecutionEnvironment env) {
		//EnviReader enviReader = env
		//		.readEnviFile(filePath, blockSize, blockSize);
		//return enviReader.restrictTo(leftUpper, rightLower).build();
		TileInputFormat<Tuple3<String, byte[], byte[]>> enviFormat = new TileInputFormat<Tuple3<String, byte[], byte[]>>(new Path(filePath));
		enviFormat.setLimitRectangle(leftUpper, rightLower);
		enviFormat.setTileSize(blockSize, blockSize);
		TupleTypeInfo<Tuple3<String, byte[], byte[]>> typeInfo = new TupleTypeInfo<Tuple3<String, byte[], byte[]>>(BasicTypeInfo.STRING_TYPE_INFO, PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO, PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO);
		return new DataSource<Tuple3<String, byte[], byte[]>>(env, enviFormat, typeInfo, "enviSource");
	}

}
