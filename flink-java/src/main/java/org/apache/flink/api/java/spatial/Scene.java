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
package org.apache.flink.api.java.spatial;

/**
 * Represents a complete Scene including possibly all the bands
 *
 */
public class Scene extends SpatialObject {
	/**
	 * 
	 */
	private static final long serialVersionUID = -937223003083256908L;

	/**
	 * Update the tile information to the given object.
	 * 
	 * @param aqcDate
	 */
	public void update(TileInfo tileInfo, Coordinate leftUpper,
			Coordinate rightLower, int width, int height, int band,
			String pathRow, String aqcDate, double xPixelWidth, 
			double yPixelWidth) {
//		this.tileInfo = tileInfo;
		this.luCord = leftUpper;
		this.rlCord = rightLower;
		this.tileWidth = width;
		this.tileHeight = height;
//		this.band = band;
		this.pathRow = pathRow;
		this.aqcuisitionDate = aqcDate;
		this.xPixelWith = xPixelWidth;
		this.yPixelWidth = yPixelWidth;
	}
}