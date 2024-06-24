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

import os
from enum import Enum
from typing import Sequence, TYPE_CHECKING

from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.operators.input import InputFileType
from airflow.operators.translateUtils import TranslateUnit
from airflow.settings import json

if TYPE_CHECKING:
    from airflow.utils.context import Context


class OutputOperator(BaseOperator):
    r"""
    Base class for all input operators.

    :param file_path: Path to the input file.
    :param file_type: Type of the input file.
    """
    template_fields: Sequence[str] = ("file_path", "file_type")
    ui_color = "#e71b64"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.file_path: str = ""
        self.file_type: InputFileType = InputFileType.TXT

    def add_format(self):
        raise NotImplementedError

    def execute(self, context: Context):
        upstream_tasks = self.get_direct_relatives(True)

        first = False
        for upstream_task in upstream_tasks:
            if first:
                raise AirflowException("more than one upstream tasks")
            first = True
            src_path = self.xcom_pull(context, upstream_task.task_id, upstream_task.dag_id, "translate")

            if not os.path.exists(src_path) or not os.path.isfile(src_path):
                raise AirflowException("upstream task folder does not exist")

            output = self.merge(src_path)
            print(output)

    def merge(self, src_path: str) -> str:
        pass



