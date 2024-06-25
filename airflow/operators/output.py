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
from typing import Sequence, TYPE_CHECKING

from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.operators.input import InputFileType
from airflow.operators.translateUtils import TranslateUnit, Paragraph
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
        self.folder_path = "./output"
        self.file_path = "output_{}.txt".format(self.task_id)
        self.file_type: InputFileType = InputFileType.TXT
        self.xcom_key = "output"

    def add_format(self):
        raise NotImplementedError

    def serialize(self, content: str, context: Context):
        if not os.path.exists(self.folder_path):
            os.makedirs(self.folder_path)
        if not os.path.isdir(self.folder_path):
            raise AirflowException("")
        with open(os.path.join(self.folder_path, self.file_path), "w") as f:
            f.write(content)
        self.xcom_push(context, self.xcom_key, os.path.join(self.folder_path, self.file_path))

    def execute(self, context: Context):
        upstream_tasks = self.get_direct_relatives(True)

        first = False
        for upstream_task in upstream_tasks:
            if first:
                raise AirflowException("more than one upstream tasks")
            first = True
            src_path = self.xcom_pull(context, upstream_task.task_id, upstream_task.dag_id,
                                      upstream_task.xcom_key)

            if not os.path.exists(src_path) or not os.path.isfile(src_path):
                raise AirflowException("upstream task folder does not exist")

            output = self.merge(src_path)
            print(output)
            self.serialize(output, context)

    def merge(self, src_path: str) -> str:
        """
        just for sentence merge
        """
        paras: list[Paragraph] = []
        with open("./src_folder/paras.txt", "r", encoding="utf-8") as f:
            input_data = json.load(f)
            for input_d in input_data:
                paras.append(Paragraph(**input_d))

        output_tus: list[TranslateUnit] = []
        with open(src_path, "r", encoding="utf-8") as f:
            input_data = json.load(f)
            for input_d in input_data:
                output_tus.append(TranslateUnit(**input_d))

        contents: list[str] = []
        start_idx = 0
        for para in paras:
            length = 0
            for idx in range(start_idx, len(output_tus)):
                if output_tus[idx].para_ids[0] != para.para_id:
                    break
                length += 1
            content = " ".join([tu.text for tu in output_tus[start_idx: start_idx + length]])
            start_idx += length
            contents.append(content)
        return "\n".join(contents)





