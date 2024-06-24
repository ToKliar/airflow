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
import os.path
import uuid
from typing import TYPE_CHECKING, Sequence

from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.operators.segment import SegmentOperator
from airflow.operators.translateUtils import TranslateEngineFactory, TranslateUnit

if TYPE_CHECKING:
    from airflow.utils.context import Context


class TranslateOperator(BaseOperator):
    r"""
    Use public translate engine to translate TU(translate unit)
    """
    template_fields: Sequence[str] = ("translate_engine_type, src_lang", "tgt_lang")
    ui_color = "#40e0cd"

    def __init__(
        self,
        *,
        translate_engine_type: str,
        src_lang: str,
        tgt_lang: str,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.translate_engine = TranslateEngineFactory.get_translate_engine(translate_engine_type, src_lang,
                                                                            tgt_lang)
        self.folder_path = "./translate"
        self.output_path = "output_{}.txt".format(uuid.uuid4())
        self.xcom_key = "translate"

    def serialize(self, data: list[TranslateUnit], context: Context) -> None:
        if not os.path.exists(self.folder_path):
            os.makedirs(self.folder_path)
            self.log.info("create folder {} to store translate outcome".format(self.folder_path))

        with open(os.path.join(self.folder_path, self.output_path), 'w') as f:
            json.dump(data, f, default=lambda o: o.__dict__, indent=4)
        self.xcom_push(context, self.xcom_key, os.path.join(self.folder_path, self.output_path))

    def execute(self, context: Context) -> None:
        if self.translate_engine is None:
            raise AirflowException("Translate engine is empty")

        upstream_tasks = self.get_direct_relatives(True)

        first = False
        for upstream_task in upstream_tasks:
            if first:
                raise AirflowException("more than one upstream tasks")
            first = True
            if not isinstance(upstream_task, SegmentOperator):
                raise AirflowException("upstream task must be SegmentOperator")
            tu_path = self.xcom_pull(context, upstream_task.task_id, upstream_task.dag_id,
                                     upstream_task.xcom_key)

            if not os.path.exists(tu_path) or not os.path.isfile(tu_path):
                raise AirflowException("upstream task folder does not exist")

            output_tus: list[TranslateUnit] = []
            with open(tu_path, "r") as f:
                input_data = json.load(f)
                for input_d in input_data:
                    input_tu = TranslateUnit(**input_d)
                    input_tu.text = self.translate_engine.translate(input_tu.text)
                    output_tus.append(input_tu)
            self.serialize(output_tus, context)
