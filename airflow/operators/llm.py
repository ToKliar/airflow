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

from enum import Enum
from typing import TYPE_CHECKING

from airflow.models.baseoperator import BaseOperator

from openai import OpenAI

if TYPE_CHECKING:
    from airflow.utils.context import Context


class LLMKind(Enum):
    Mock = 0
    OpenAI = 1
    QWen = 2
    Local = 3


class LLMEngine:
    r"""
    Operator to call LLM
    """

    # TODO: first mock the output, second call Qwen

    def get_api_key(self) -> str:
        raise NotImplementedError

    def build_request(self, message: list[str]) -> str:
        raise NotImplementedError

    def get_response(self, message: list[str]) -> str:
        raise NotImplementedError


class QWenLLMEngine(LLMEngine):
    # 填写DashScope SDK的base_url
    base_url = "https://dashscope.aliyuncs.com/compatible-mode/v1"


def get_llm_engine(kind: LLMKind) -> LLMEngine:
    if kind == LLMKind.Mock:
        return LLMEngine()
    raise NotImplementedError
