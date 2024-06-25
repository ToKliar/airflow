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
from typing import TYPE_CHECKING

from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.operators.choose import ChooseOperator, ChooseOutput
from airflow.operators.output import OutputOperator

from airflow.operators.translate import TranslateOperator
from airflow.operators.translateUtils import TranslateUnit

if TYPE_CHECKING:
    from airflow.utils.context import Context


class ProofOperator(BaseOperator):
    r"""
    Operator to call LLM
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.folder_path = "./proof"
        self.output_path = "proof_{}.txt".format(self.task_id)
        self.xcom_key = "proof"

    def serialize(self, data: str, context: Context) -> None:
        if not os.path.exists(self.folder_path):
            os.makedirs(self.folder_path)
            self.log.info("create folder {} to store proof outcome".format(self.folder_path))

        with open(os.path.join(self.folder_path, self.output_path), 'w') as f:
            f.write(data)
        self.xcom_push(context, self.xcom_key, os.path.join(self.folder_path, self.output_path))

    def proof(self, src: str) -> str:
        return src

    def execute(self, context: Context) -> None:
        upstream_tasks = self.get_direct_relatives(True)

        for upstream_task in upstream_tasks:
            if isinstance(upstream_task, OutputOperator):
                output_path = self.xcom_pull(context, upstream_task.task_id, upstream_task.dag_id,
                                             upstream_task.xcom_key)

                if not os.path.exists(output_path) or not os.path.isfile(output_path):
                    raise AirflowException("upstream task folder does not exist")

                with open(output_path, "r") as f:
                    output_data = self.proof("\n".join(f.readlines()))
                self.serialize(output_data, context)
                break
            elif isinstance(upstream_task, ChooseOperator):
                choose_path = self.xcom_pull(context, upstream_task.task_id, upstream_task.dag_id,
                                             upstream_task.xcom_key)

                if not os.path.exists(choose_path) or not os.path.isfile(choose_path):
                    raise AirflowException("upstream task folder does not exist")

                with open(choose_path, "r") as f:
                    output_data = json.load(f)
                    for output_d in output_data:
                        output_txt = ChooseOutput(**output_d)
                        if output_txt.rank == 1:
                            output_txt.content = self.proof(output_txt.content)
                            self.serialize(output_txt.content, context)
                            break
                break
            raise AirflowException("upstream task must be Translate Operator or Choose Operator")
