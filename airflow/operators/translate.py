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

from typing import TYPE_CHECKING, Sequence

from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.operators.translateUtils import TranslateEngineFactory

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

    def execute(self, context: Context) -> list[str]:
        if self.translate_engine is None:
            raise AirflowException("Translate engine is empty")
        src_data = self.get_data()
        output = []
        for data in src_data:
            output.append(self.translate_engine.translate(data))
        return output

    def get_data(self) -> list[str]:
        """
        get source data from prev operator
        """
        raise NotImplementedError
