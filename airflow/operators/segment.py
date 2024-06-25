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
from typing import TYPE_CHECKING

from airflow.models.baseoperator import BaseOperator
from airflow.operators.translateUtils import TranslateUnit, Paragraph, Sentence

if TYPE_CHECKING:
    from airflow.utils.context import Context


class SegmentOperator(BaseOperator):
    r"""
    Base class for all segment operators.
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.docs: list[str] = []
        self.params: list[Paragraph] = []
        self.sentences: list[Sentence] = []

        self.folder_path = './seg_folder_' + self.task_id
        self.src_folder_path = './src_folder'
        self.docs_path = './docs.txt'
        self.paras_path = './paras.txt'
        self.sentences_path = './sentences.txt'
        self.tu_path = './tu.txt'
        self.xcom_key = "segment"

    def pre_segment(self):
        from nltk.tokenize import sent_tokenize

        with open("./source_file.txt", "r", encoding="utf-8") as f:
            self.docs = ["".join(f.readlines())]
        para_id = 0
        sent_id = 0
        for doc_id, doc in enumerate(self.docs):
            params = doc.split("\n")
            for r_para_id, param in enumerate(params):
                self.params.append(Paragraph(doc_id, param, para_id, r_para_id))
                para_id += 1
                sentences = sent_tokenize(param)
                for r_sent_id, sentence in enumerate(sentences):
                    self.sentences.append(Sentence(sentence, sent_id, para_id, r_sent_id))
                    sent_id += 1

    def execute(self, context: Context):
        """
        segment raw text to translate units
        """
        if not os.path.exists(self.src_folder_path):
            os.makedirs(self.src_folder_path)
            self.log.info("create folder {} to store source text parts".format(self.src_folder_path))
        self.pre_segment()
        self.storage()

    def storage(self):
        with open(os.path.join(self.src_folder_path, self.docs_path), "w", encoding="utf-8") as f:
            json.dump(self.docs, f, default=lambda o: o.__dict__, indent=4)
            f.close()

        with open(os.path.join(self.src_folder_path, self.paras_path), "w", encoding="utf-8") as f:
            json.dump(self.params, f, default=lambda o: o.__dict__, indent=4)
            f.close()

        with open(os.path.join(self.src_folder_path, self.sentences_path), "w", encoding="utf-8") as f:
            json.dump(self.sentences, f, default=lambda o: o.__dict__, indent=4)
            f.close()

    def serialize(self, data: list[TranslateUnit], context: Context) -> None:
        if not os.path.exists(self.folder_path):
            os.makedirs(self.folder_path)
            self.log.info("create folder {} to store translated units".format(self.folder_path))

        with open(os.path.join(self.folder_path, self.tu_path), 'w') as f:
            json.dump(data, f, default=lambda o: o.__dict__, indent=4)
        self.xcom_push(context, self.xcom_key, os.path.join(self.folder_path, self.tu_path))


class SentenceSegmentOperator(SegmentOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def execute(self, context: Context) -> None:
        super().execute(context)
        output: list[TranslateUnit] = []
        for sentence in self.sentences:
            output.append(TranslateUnit([sentence.para_id], [sentence.sent_id], sentence.content))

        self.serialize(output, context)
