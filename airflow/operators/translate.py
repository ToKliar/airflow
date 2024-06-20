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

import os.path
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
        self.file_path = ""
        self.xcom_key = "file_folder"

    def serialize(self, data: list[TranslateUnit], context: Context) -> None:
        if not os.path.exists(self.file_path):
            os.makedirs(os.path.dirname(self.file_path), exist_ok=True)
            self.log.info("create folder {} to store translated units".format(self.file_path))

        for tu_id, tu in enumerate(data):
            with open(os.path.join(self.file_path, str(tu_id) + '.txt'), 'w') as f:
                f.write(tu.serialize(tu_id))
        self.xcom_push(context, self.xcom_key, self.file_path)

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
            folder = self.xcom_pull(context, upstream_task.task_id, upstream_task.dag_id,
                                    upstream_task.xcom_key)

            if not os.path.exists(folder) or not os.path.isdir(folder):
                raise AirflowException("upstream task folder does not exist")

            output_tus: list[TranslateUnit] = []
            for file in os.listdir(folder):
                file_path = os.path.join(folder, file)
                with open(file_path, "r") as f:
                    input_data = f.readlines()
                    for input_line in input_data:
                        input_tu = TranslateUnit.deserialize(input_line)
                        if not input_tu.skip():
                            input_tu.text = self.translate_engine.translate(input_tu.text)
                        output_tus.append(input_tu)
            self.serialize(output_tus, context)
