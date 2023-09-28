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

from granule_ingester.pipeline import Pipeline
import logging
import pickle
import time
from multiprocessing import Manager
from typing import List

import yaml
from aiomultiprocess import Pool
from aiomultiprocess.types import ProxyException
from granule_ingester.exceptions import PipelineBuildingError
from granule_ingester.granule_loaders import GranuleLoader
from granule_ingester.pipeline.Modules import \
    modules as processor_module_mappings
from granule_ingester.processors.TileProcessor import TileProcessor
from granule_ingester.slicers import TileSlicer
from granule_ingester.writers import DataStore, MetadataStore, SolrStore
from tblib import pickling_support
import numpy as np
import json
from itertools import chain
from nexusproto.DataTile_pb2 import TileSummary

logger = logging.getLogger(__name__)


class CoGPipeline(Pipeline):
    def __init__(self,
                 granule_loader: GranuleLoader,
                 metadata_store_factory,
                 dataset: str,
                 granule_s: str,
                 dims: dict,
                 config: dict,
                 log_level=logging.INFO):
        self._granule_loader = granule_loader
        self._metadata_store_factory = metadata_store_factory
        self._dataset = dataset
        self._granule = granule_s
        self._dims = dims
        self._level = log_level
        self._config = config

    def set_log_level(self, level):
        self._level = level

    @classmethod
    def build_pipeline(cls,
                       config: dict,
                       data_store_factory,
                       metadata_store_factory,
                       module_mappings: dict,
                       max_concurrency: int,
                       ):
        granule_loader = GranuleLoader(**config['granule'], tiff=True, dims=config['dimensions'])

        return cls(
            granule_loader,
            metadata_store_factory,
            config['dataset_name'],
            config['granule']['granule_s'],
            config['dimensions'],
            config.get('config', {})
        )

    async def run(self):
        # Note: When pushing to Solr, use granule path from config dict

        bounds = {}

        async with self._granule_loader as (dataset, granule_name):
            time = np.datetime_as_string(
                dataset['time'].to_numpy().flatten(),
                unit='s',
                timezone='UTC'
            ).tolist()
            lats = dataset['y'].to_numpy().flatten()
            lons = dataset['x'].to_numpy().flatten()

            # elevation = None
            #
            # if 'height' in self._dims:
            #     elevation = dataset[self._dims['height']].to_numpy().flatten()
            # elif 'elevation' in self._dims:
            #     elevation = dataset[self._dims['elevation']].to_numpy().flatten()
            # elif 'depth' in self._dims:
            #     elevation = dataset[self._dims['depth']].to_numpy().flatten() * -1

            bounds['max_time_dt'] = max(time)
            bounds['min_time_dt'] = min(time)

            bounds['max_lon_d'] = np.nanmax(lons)
            bounds['min_lon_d'] = np.nanmin(lons)

            bounds['max_lat_d'] = np.nanmax(lats)
            bounds['min_lat_d'] = np.nanmin(lats)

            bbox = TileSummary().bbox

            bbox.lat_max = bounds['max_lat_d']
            bbox.lat_min = bounds['min_lat_d']
            bbox.lon_max = bounds['max_lon_d']
            bbox.lon_min = bounds['min_lon_d']

            geo = SolrStore.determine_geo(bbox)

            # if elevation:
            #     bounds['max_elevation_l'] = np.nanmax(elevation)
            #     bounds['min_elevation_l'] = np.nanmin(elevation)

        await self._metadata_store_factory().save_granule(
            self._granule,
            self._dataset,
            bounds,
            self._granule_loader.get_resource(),
            geo
        )

        variables = json.loads(self._dims['variable'])

        config = {
            'bands': dict(chain(*[d.items() for d in variables]))
        }

        if self._config:
            config['config'] = self._config

        await self._metadata_store_factory().update_dataset(self._dataset, 'cog', config)
