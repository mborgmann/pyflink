################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
from abc import ABCMeta, abstractmethod
import sys
from collections import deque
from flink.connection import Connection, Iterator, Collector
from flink.functions import RuntimeContext
import os, sys, time, gdal
import numpy as np
from gdalconst import *
import __builtin__ as builtins

class PythonInputFormat(object):
    
    def __init__(self, path):
        self.path = path
        self._connection = None
        self._iterator = None
        self._collector = None
        self.context = None
        self._chain_operator = None
        gdal.AllRegister() #TODO: register the ENVI driver only
    
    def _run(self):
        collector = self._collector
        function = self.deliver
        for value in self._iterator:
            function(value, collector)
        collector._close()
        
    def deliver(self, path, collector):

        ds = gdal.Open(path[5:], GA_ReadOnly)
        if ds is None:
            print 'Could not open image'
            return
        else:
            print 'opened image successfully'
        rows = ds.RasterYSize
        cols = ds.RasterXSize
        bandsize = rows * cols
        bands = ds.RasterCount

        imageData = np.empty(bands * bandsize)
        for j in range(bands):
            band = ds.GetRasterBand(j+1)
            data = np.array(band.ReadAsArray())
            lower = j*bandsize
            upper = (j+1)*bandsize
            imageData[lower:upper] = data.ravel()
        
        metaData = self.readMetaData(path[5:-4])
        print metaData


    def _configure(self, input_file, output_file, port):
        self._connection = Connection.BufferingTCPMappedFileConnection(input_file, output_file, port)
        self._iterator = Iterator.Iterator(self._connection)
        self.context = RuntimeContext.RuntimeContext(self._iterator, self._collector)
        self._configure_chain(Collector.Collector(self._connection))

    def _configure_chain(self, collector):
        if self._chain_operator is not None:
            self._collector = self._chain_operator
            self._collector.context = self.context
            self._collector._configure_chain(collector)
            self._collector._open()
        else:
            self._collector = collector
            
    def open(self, inputsplit):
        self.split = inputsplit
    
    def close(self):
        self.close()
    
    def _go(self):
        self._receive_broadcast_variables()
        self._run()

    def _receive_broadcast_variables(self):
        broadcast_count = self._iterator.next()
        self._iterator._reset()
        self._connection.reset()
        for _ in range(broadcast_count):
            name = self._iterator.next()
            self._iterator._reset()
            self._connection.reset()
            bc = deque()
            while(self._iterator.has_next()):
                bc.append(self._iterator.next())
            self.context._add_broadcast_variable(name, bc)
            self._iterator._reset()
            self._connection.reset()

    def readMetaData(self, path):
        headerPath = path+'.hdr'
        f = builtins.open(headerPath, 'r')

        if f.readline().find("ENVI") == -1:
            f.close()
            raise IOError("Not an ENVI header.")

        lines = f.readlines()
        f.close()

        dict = {}
        try:
            while lines:
                line = lines.pop(0)
                if line.find('=') == -1: continue
                if line[0] == ';': continue

                (key, sep, val) = line.partition('=')
                key = key.strip().lower()
                val = val.strip()
                if val and val[0] == '{':
                    str = val.strip()
                    while str[-1] != '}':
                        line = lines.pop(0)
                        if line[0] == ';': continue

                        str += '\n' + line.strip()
                    if key == 'description':
                        dict[key] = str.strip('{}').strip()
                    else:
                        vals = str[1:-1].split(',')
                        for j in range(len(vals)):
                            vals[j] = vals[j].strip()
                        dict[key] = vals
                else:
                    dict[key] = val
            return dict
        except:
            raise IOError("Error while reading ENVI file header.")