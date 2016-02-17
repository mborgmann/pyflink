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

package org.apache.flink.languagebinding.api.java.python.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.languagebinding.api.java.python.streaming.PythonStreamer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An input format to parse ENVI files into Tile objects.
 * Every input file is split into adjacent tiles of the given size.
 * Missing pixels are filled with the missing value specified in the ENVI file.
 * 
 * @author Dennis Schneider <dschneid@informatik.hu-berlin.de>
 * @author Mathias Peters <mathias.peters@informatik.hu-berlin.de>
 */
//public class EnviInputFormat<T extends Tile> extends FileInputFormat<T> {
public class PythonInputFormat<T extends Tuple> extends SpatialInputFormat<T> implements ResultTypeQueryable<T> {	
	
	private PythonStreamer streamer;
	private boolean streamerOpen;
	static final Logger LOG = LoggerFactory.getLogger(PythonInputFormat.class);
	private TypeInformation<T> typeInformation;
	private Path path;
	private Configuration configuration;
	
	
	public void setTypeInformation(TypeInformation<T> typeInformation) {
		this.typeInformation = typeInformation;
	}

	public PythonInputFormat(Path path, int id, TypeInformation<T> info) {
		super(path);
		this.path = path;
		this.completeScene = true;
		this.streamer = new PythonStreamer(this, id);
		this.typeInformation = info;
	} 
	
	@Override
	public T nextRecord(T record) throws IOException {
		if(!this.streamerOpen) {
			this.streamer.open();
			this.streamer.sendBroadCastVariables(this.configuration);
			this.streamerOpen = true;
		}
		
		if (this.reachedEnd()) {
			return null;
		}
		
		record = readEnviTile(record);
		this.readRecords++;
		return record;
	}
	
	@Override
	public void configure(Configuration parameters) {
		// TODO Auto-generated method stub
		super.configure(parameters);
		this.configuration = parameters;
	}

	private T readEnviTile(T record) throws IOException {
		//use streamer here
		PythonCollector<T> collector = new PythonCollector<>();
		List<String> pathList = new ArrayList<String>();
		pathList.add(this.path.toString());
		this.streamer.streamBufferWithoutGroups(pathList.iterator(), collector);
		
		record = collector.poll(); //TODO: fix logic of multiple returns per split
		
		return record;
	}

	@Override
	public TypeInformation<T> getProducedType() {
		return this.typeInformation;
	}
	
	@Override
	public void open(FileInputSplit split) throws IOException {
		// TODO Auto-generated method stub
		super.open(split);
		this.path = split.getPath();
	}

}
