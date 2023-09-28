# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
from abc import ABC, abstractmethod

import yaml
import json
from granule_ingester.exceptions import PipelineBuildingError
from granule_ingester.pipeline import NexusprotoPipeline, CoGPipeline, Pipeline
from granule_ingester.pipeline.Modules import \
    modules as processor_module_mappings

logger = logging.getLogger(__name__)


class PipelineBuilder():
    @staticmethod
    def from_string(config_str: str, data_store_factory, metadata_store_factory, max_concurrency: int = 16) -> Pipeline:
        logger.debug(f'config_str: {config_str}')
        try:
            config = yaml.load(config_str, yaml.FullLoader)

            if 'time_dict' in config['dimensions']:
                config['dimensions']['time'] = json.loads(config['dimensions']['time_dict'])
                del config['dimensions']['time_dict']

            PipelineBuilder._validate_config(config)

            pipeline_class = None

            if 'pipeline_type' not in config or config['pipeline_type'].lower() == 'nexusproto':
                pipeline_class = NexusprotoPipeline
            elif config['pipeline_type'].lower() in ['cog', 'cloud_optimized_geotiff']:
                pipeline_class = CoGPipeline
            else:
                raise ValueError(f'Unsupported pipeline_type: {config["pipeline_type"]}')

            return pipeline_class.build_pipeline(config,
                                   data_store_factory,
                                   metadata_store_factory,
                                   processor_module_mappings,
                                   max_concurrency)

        except yaml.scanner.ScannerError:
            raise PipelineBuildingError("Cannot build pipeline because of a syntax error in the YAML.")

    @staticmethod
    def _validate_config(config: dict):
        if type(config) is not dict:
            raise PipelineBuildingError("Cannot build pipeline; the pipeline configuration that " +
                                        "was received is not valid YAML.")
