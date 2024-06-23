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
from __future__ import annotations

import hashlib
import json
import time
import uuid

import requests

from airflow.exceptions import AirflowException


class TranslateUnit:
    def __init__(self, doc_ids: list[int], para_ids: list[int], sent_ids: list[int], text: str,
                 empty: bool = False):
        self.idx = None
        self.doc_ids = doc_ids
        self.para_ids = para_ids
        self.sent_ids = sent_ids
        self.text = text
        self.empty = empty

    def skip(self) -> bool:
        return self.empty or (len(self.sent_ids) == 1 and self.sent_ids[0] == TranslateUnit.EMPTY_SENT_ID)

    def serialize(self, idx: int) -> str:
        self.idx = idx
        out_str = "\t".join(
            [str(self.idx), str(self.doc_ids), str(self.para_ids), str(self.sent_ids), self.text,
             str(self.empty)])
        return out_str

    @staticmethod
    def deserialize(data: str) -> TranslateUnit:
        parts = data.split("\t")
        if len(parts) <= 5:
            raise AirflowException("Translate Unit is not valid")
        try:
            idx = int(parts[0])
            doc_ids = [int(idx) for idx in list(parts[1])]
            para_ids = [int(idx) for idx in list(parts[2])]
            sent_ids = [int(idx) for idx in list(parts[3])]
            text = parts[4]
            empty = bool(parts[5])
            tu = TranslateUnit(doc_ids, para_ids, sent_ids, text, empty)
            tu.idx = idx
            return tu
        except Exception as e:
            raise AirflowException("Translate Unit is not valid due to {} when deserialize {}"
                                   .format(repr(e), data))

    EMPTY_SENT_ID = -1


class TranslateEngineFactory:
    @staticmethod
    def get_translate_engine(kind: str = 'youdao', source_language: str = "English",
                             target_language: str = "Chinese") -> TranslateEngine:
        if kind == 'youdao':
            return YouDaoTranslateEngine(source_language, target_language)
        if kind == "google":
            return GoogleTranslateEngine(source_language, target_language)
        if kind == "deepl":
            return DeepLTranslateEngine(source_language, target_language)

        raise ValueError(f"Translate engine kind {kind} not supported")


class TranslateEngine:
    r"""
    Base class of translate engine caller

    :param source_language:
    :param target_language:
    """

    def __init__(self, source_language: str, target_language: str):
        self.translate_kind = "default"
        self.source_language = source_language
        self.target_language = target_language

    def translate(self, source: str, **kwargs) -> str:
        raise NotImplementedError

    def get_api_key(self) -> str:
        raise NotImplementedError

    def get_api_secret(self) -> str:
        raise NotImplementedError


class GoogleTranslateEngine(TranslateEngine):
    def __init__(self, source_language: str, target_language: str):
        super().__init__(source_language, target_language)
        self.translate_kind = "google"

    # TODO: use google translate API
    def translate(self, source: str, **kwargs) -> str:
        raise NotImplementedError


class DeepLTranslateEngine(TranslateEngine):
    def __init__(self, source_language: str, target_language: str):
        super().__init__(source_language, target_language)
        self.translate_kind = "deepl"

    # TODO: use DeepL translate API:
    def translate(self, source: str, **kwargs) -> str:
        raise NotImplementedError


def truncate(q: str):
    if q is None:
        return None
    size = len(q)
    return q if size <= 20 else q[0:10] + str(size) + q[size - 10:size]


def encrypt(sign_str: str):
    hash_algorithm = hashlib.sha256()
    hash_algorithm.update(sign_str.encode('utf-8'))
    return hash_algorithm.hexdigest()


class YouDaoTranslateEngine(TranslateEngine):
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    youdao_url = "https://openapi.youdao.com/api"
    app_key = "26d53e2a45215902"
    app_secret = "sIoaQsfUo4nFxVmnWMIEZq0rCkqMX8J6"

    def __init__(self, source_language: str, target_language: str):
        super().__init__(source_language, target_language)
        self.translate_kind = "youdao"

    def get_api_key(self) -> str:
        return YouDaoTranslateEngine.app_key

    def get_api_secret(self) -> str:
        return YouDaoTranslateEngine.app_secret

    def translate(self, source: str, **kwargs) -> str:
        api_key = self.get_api_key()
        api_secret = self.get_api_secret()

        data = dict()
        data['from'] = self.source_language
        data['to'] = self.target_language
        data['q'] = source
        data['appKey'] = api_key

        cur_time = str(int(time.time()))
        data['curtime'] = cur_time

        # sign and encrypt data
        data['signType'] = 'v3'
        salt = str(uuid.uuid1())
        sign_str = (api_key + truncate(source) + salt + cur_time + api_secret)
        sign = encrypt(sign_str)
        data['salt'] = salt
        data['sign'] = sign

        try_count = 0
        while try_count < 5:
            try_count += 1
            try:
                response = requests.post(YouDaoTranslateEngine.youdao_url, data=data,
                                         headers=YouDaoTranslateEngine.headers)
                if response.status_code != 200:
                    continue
                content_type = response.headers['Content-Type']
                if content_type is None or content_type == 'audio/mp3':
                    continue
                json_data = json.loads(response.text)
                if (json_data['errorCode'] != '0' or json_data['translation'] is None or
                    not isinstance(json_data['translation'], list)):
                    continue
                return "".join(json_data['translation'])
            except Exception:
                continue
