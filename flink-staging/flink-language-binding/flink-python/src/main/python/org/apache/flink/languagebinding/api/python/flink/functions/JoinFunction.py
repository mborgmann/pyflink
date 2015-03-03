# ###############################################################################
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
from flink.functions import Function


class JoinFunction(Function.Function):
    def __init__(self):
        super(JoinFunction, self).__init__()
        self._discard_key1 = False
        self._discard_key2 = False

    def _run(self):
        collector = self._collector
        function = self.join
        discard1 = self._discard_key1
        discard2 = self._discard_key2
        for value in self._iterator:
            collector.collect(function(value[0][1] if discard1 else value[0], value[1][1] if discard2 else value[1]))
        collector._close()

    def join(self, value1, value2):
        pass
