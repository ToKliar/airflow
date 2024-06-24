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
from typing import Sequence, TYPE_CHECKING

from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.utils.types import ArgNotSet

if TYPE_CHECKING:
    from airflow.utils.context import Context


class InputFileType(Enum):
    TXT = 1
    PDF = 2
    EPUB = 3


class InputBaseOperator(BaseOperator):
    r"""
    Base class for all input operators.

    :param file_path: Path to the input file.
    :param file_type: Type of the input file.
    """
    template_fields: Sequence[str] = ("file_path", "file_type")
    ui_color = "#e71b64"

    def __init__(self, file_path: str | ArgNotSet, file_type: InputFileType = InputFileType.TXT, **kwargs) -> None:
        super().__init__(**kwargs)
        self.file_path = file_path
        self.file_type = file_type

    def extract_content(self) -> str:
        raise NotImplementedError

    def store_format(self):
        raise NotImplementedError

    def get_text_path(self) -> str:
        return "./source_file.txt"

    def execute(self, context: Context):
        """
        get raw text from input file path or folder path
        """
        raw_source = self.extract_content()
        raw_path = self.get_text_path()
        with open(raw_path, "w", encoding="utf-8") as f:
            f.write(raw_source)

        self.xcom_push(context, "source_path", raw_path)
        self.xcom_push(context, "file_type", self.file_type)


class InputTxtOperator(InputBaseOperator):
    def __init__(self, *, file_path: str, file_type: InputFileType = InputFileType.TXT, **kwargs) -> None:
        super().__init__(file_path, file_type, **kwargs)

    def extract_content(self) -> str:
        with open(self.file_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
            return "\n".join(lines)
