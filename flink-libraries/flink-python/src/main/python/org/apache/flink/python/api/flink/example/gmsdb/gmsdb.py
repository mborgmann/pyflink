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
from __future__ import print_function

import sys
from flink.plan.Environment import get_environment
from flink.gmsdb.GMSPreprocessInputFormat import GMSDB
from flink.gmsdb.L1Processor import L11Processor, CornerpointAdder, L12Processor
from flink.functions.FilterFunction import FilterFunction

#purely to filter out everything
#this way we can run programs w/o proper output format
class DumbFilter(FilterFunction):
    def filter(self, value):
        return False


def main():
    try:
        data_path = sys.argv[1]
    except IndexError:
        data_path = "/data1/gfz-fe/GeoMultiSens/database/sampledata/"

    connection = {
            "database": "usgscache",
            "user": "gmsdb",
            "password": "gmsdb",
            "host": "localhost",
            "connect_timeout": 3,
            "options": "-c statement_timeout=10000"
        }

    inputFormat = GMSDB(data_path, connection, 26184107)

    env = get_environment()
    level0Set = env.read_custom(data_path, ".*?\\.bsq", True, inputFormat)
    level1Set = level0Set.flat_map(L11Processor())
    level1SceneSet = level1Set.group_by(0).reduce()
    level12Set = level1SceneSet.flat_map(L12Processor())

    #just to make program complete
    result = level12Set.filter(DumbFilter())
    result.output()

    env.set_degree_of_parallelism(1)

    env.execute(local=True)


if __name__ == "__main__":
    main()
