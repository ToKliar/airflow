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
from typing import TYPE_CHECKING

from airflow.models.baseoperator import BaseOperator
from airflow.operators.translateUtils import TranslateUnit

if TYPE_CHECKING:
    from airflow.utils.context import Context


class SegmentOperator(BaseOperator):
    r"""
    Base class for all segment operators.
    """

    def __init__(self, *, docs: list[str], file_path: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.docs = docs
        self.file_path = file_path
        self.xcom_key = "file_folder"

    def execute(self, context: Context) -> list[TranslateUnit]:
        """
        segment raw text to translate units
        """
        raise NotImplementedError

    def serialize(self, data: list[TranslateUnit], context: Context) -> None:
        folder = os.path.exists(self.file_path)
        if not folder:
            os.makedirs(os.path.dirname(self.file_path), exist_ok=True)
            self.log.info("create folder {} to store translated units".format(self.file_path))

        for tu_id, tu in enumerate(data):
            with open(os.path.join(self.file_path, str(tu_id) + '.txt'), 'w') as f:
                f.write(tu.serialize(tu_id))
        self.xcom_push(context, self.xcom_key, self.file_path)


class SentenceSegmentOperator(SegmentOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def execute(self, context: Context) -> None:
        from nltk.tokenize import sent_tokenize
        output: list[TranslateUnit] = []
        for doc_idx, doc in enumerate(self.docs):
            params = doc.split("\n")
            for para_idx, param in enumerate(params):
                sentences = sent_tokenize(param)
                if len(sentences) == 0:
                    output.append(TranslateUnit([doc_idx], [para_idx], [TranslateUnit.EMPTY_SENT_ID], ""))
                for sent_idx, sent in enumerate(sentences):
                    output.append(TranslateUnit([doc_idx], [para_idx], [sent_idx], sent, len(sent) == 0))
        self.serialize(output, context)
