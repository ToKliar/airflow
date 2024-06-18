#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# fmt: off
from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.models.baseoperator import BaseOperator
from airflow.operators.translateUtils import TranslateUnit

if TYPE_CHECKING:
    from airflow.utils.context import Context


class SegmentOperator(BaseOperator):
    r"""
    Base class for all segment operators.
    """

    def __init__(self, *, raw_txt: str, **kwargs) -> None:
        self.raw_txt = raw_txt
        super().__init__(**kwargs)

    def execute(self, context: Context) -> list[TranslateUnit]:
        """
        segment raw text to translate units
        """
        raise NotImplementedError





