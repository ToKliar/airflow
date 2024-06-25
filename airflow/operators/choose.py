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

import json
import os
import random
from typing import TYPE_CHECKING

from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.operators.output import OutputOperator

from airflow.operators.translate import TranslateOperator
from airflow.operators.translateUtils import TranslateUnit

if TYPE_CHECKING:
    from airflow.utils.context import Context


class ChooseOutput:
    def __init__(self, content: str, rank: int, total: int, comment: str):
        self.content = content
        self.rank = rank
        self.total = total
        self.comment = comment


class ChooseOperator(BaseOperator):
    r"""
    Operator to call LLM
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.folder_path = "./choose"
        self.output_path = "output_{}.txt".format(self.task_id)
        self.xcom_key = "choose"

    def serialize(self, data: list[ChooseOutput], context: Context) -> None:
        if not os.path.exists(self.folder_path):
            os.makedirs(self.folder_path)
            self.log.info("create folder {} to store proof outcome".format(self.folder_path))

        with open(os.path.join(self.folder_path, self.output_path), 'w') as f:
            json.dump(data, f, default=lambda o: o.__dict__, indent=4)
        self.xcom_push(context, self.xcom_key, os.path.join(self.folder_path, self.output_path))

    def choose(self, texts: list[str]) -> list[ChooseOutput]:
        total = len(texts)
        rank_list = list(range(1, total + 1))
        random.shuffle(rank_list)
        output: list[ChooseOutput] = []
        for idx, text in enumerate(texts):
            output.append(ChooseOutput(text, rank_list[idx], total, str(rank_list[idx])))
        return output

    def execute(self, context: Context) -> None:
        upstream_tasks = self.get_direct_relatives(True)

        choose_contents: list[str] = []

        for upstream_task in upstream_tasks:
            if not isinstance(upstream_task, OutputOperator):
                raise AirflowException("upstream task must be OutputOperator")
            output_path = self.xcom_pull(context, upstream_task.task_id, upstream_task.dag_id,
                                         upstream_task.xcom_key)

            if not os.path.exists(output_path) or not os.path.isfile(output_path):
                raise AirflowException("upstream task folder does not exist")

            with open(output_path, "r") as f:
                choose_contents.append("\n".join(f.readlines()))

        choose_output = self.choose(choose_contents)
        self.serialize(choose_output, context)
