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


class TranslateUnit:
    pass


class TranslateEngineFactory:
    @staticmethod
    def get_translate_engine(kind: str, source_language: str = "English",
                             target_language: str = "Chinese") -> TranslateEngine:
        if kind == "google":
            return GoogleTranslateEngine(source_language, target_language)
        elif kind == "deepl":
            return DeepLTranslateEngine(source_language, target_language)
        else:
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
