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


class InputBaseOperator(BaseOperator):
    r"""
    Base class for all input operators.

    :param file_path: Path to the input file.
    :param file_type: Type of the input file.
    """
    template_fields: Sequence[str] = ("file_path", "file_type")
    ui_color = "#e71b64"

    def __init__(self, *, file_path: str | ArgNotSet, kind: InputFileType = InputFileType.TXT, **kwargs) -> None:
        super().__init__(**kwargs)
        self.file_path = file_path
        self.file_type = kind

    def execute(self, context: Context) -> str:
        """
        get raw text from input file path or folder path
        """
        if self.file_path is not None and self.file_type == InputFileType.TXT:
            try:
                with open(self.file_path, "r", encoding="utf-8") as f:
                    output = f.readlines()
                    return "".join(output)
            except FileNotFoundError:
                raise AirflowException("File not found")
            except UnicodeDecodeError:
                raise AirflowException("File not decoded")
        raise NotImplementedError()
