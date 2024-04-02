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
import os
import tempfile
import re
from urllib import parse
import json
from itertools import chain
from datetime import datetime

import aioboto3
import boto3
import rioxarray
import rasterio as rio
from rasterio.session import AWSSession
import xarray as xr
from granule_ingester.exceptions import GranuleLoadingError, PipelineBuildingError
from granule_ingester.granule_loaders.Preprocessors import modules as module_mappings
from granule_ingester.preprocessors import GranulePreprocessor
from pathlib import PurePosixPath
import numpy as np
from rioxarray.exceptions import MissingCRS

logger = logging.getLogger(__name__)


class GranuleLoader:

    def __init__(self, resource: str, *args, **kwargs):
        self._granule_temp_file = None
        self._resource = resource
        self._preprocess = None

        self._tiff = False

        if 'group' in kwargs:
            self._group = kwargs['group']
        else:
            self._group = None

        if 'preprocess' in kwargs:
            self._preprocess = [GranuleLoader._parse_module(module) for module in kwargs['preprocess']]

        if 'tiff' in kwargs:
            self._tiff = True
            variables = json.loads(kwargs['dims']['variable'])

            self._dims = kwargs['dims']
            self._vars = dict(chain(*[d.items() for d in variables]))

    async def __aenter__(self):
        return await self.open()

    async def __aexit__(self, type, value, traceback):
        if self._granule_temp_file:
            self._granule_temp_file.close()

    def get_resource(self):
        return self._resource

    async def open(self) -> (xr.Dataset, str):
        resource_url = parse.urlparse(self._resource)
        if resource_url.scheme == 's3':
            if not self._tiff:
                # We need to save a reference to the temporary granule file so we can delete it when the context manager
                # closes. The file needs to be kept around until nothing is reading the dataset anymore.
                self._granule_temp_file = await self._download_s3_file(self._resource)
                file_path = self._granule_temp_file.name
            else:
                file_path = self._resource
        elif resource_url.scheme == '':
            file_path = self._resource
        else:
            raise RuntimeError("Granule path scheme '{}' is not supported.".format(resource_url.scheme))

        granule_name = os.path.basename(self._resource)
        try:
            if self._tiff:
                def determine_time(granule_path: str):
                    time_d = self._dims['time']

                    if isinstance(time_d, dict):
                        if 'field' in time_d:
                            field = time_d['field']

                            time_string = re.split(
                                field['delimiter'],
                                os.path.basename(granule_path)
                            )[field['n']]
                        elif 'index' in time_d:
                            idx = time_d['index']

                            time_string = os.path.basename(granule_path)[idx['start']:idx['end']]
                        else:
                            raise ValueError('Time dimension imporperly configured')

                        time_dt = datetime.strptime(time_string, time_d['format'])

                        return np.datetime64(time_dt)
                    else:
                        logger.warning('No clear definition of tiff time given, using default')

                        time_date_str = PurePosixPath(granule_path).stem.split('_')[-1]
                        time_parts = time_date_str.partition('T')
                        time_str = time_parts[2]
                        date_str = time_parts[0]
                        time_date_str = (f'{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}T'
                                         f'{time_str[:2]}:{time_str[2:4]}:{time_str[4:]}').rstrip('Z')

                        time_val = np.datetime64(time_date_str)

                        return time_val

                granule_time = determine_time(granule_name)

                if resource_url.scheme != 's3':
                    tiff = rioxarray.open_rasterio(file_path, mask_and_scale=True, band_as_variable=True)
                else:
                    session = boto3.session.Session()

                    with rio.Env(
                        AWSSession(session),
                        GDAL_DISABLE_READDIR_ON_OPEN='EMPTY_DIR'
                    ):
                        tiff = rioxarray.open_rasterio(file_path, mask_and_scale=True, band_as_variable=True)

                try:
                    tiff = tiff.rio.reproject(dst_crs='EPSG:4326')
                except MissingCRS:
                    tiff = tiff.rio.write_crs('EPSG:4326').rio.reproject(dst_crs='EPSG:4326', nodata=np.nan)

                tiff.expand_dims({"time": 1})
                tiff = tiff.assign_coords({"time": [granule_time]})

                return tiff, granule_name
            else:
                additional_params = {}

                if self._group is not None:
                    additional_params['group'] = self._group

                ds = xr.open_dataset(file_path, lock=False, **additional_params)

                if self._preprocess is not None:
                    logger.info(f'There are {len(self._preprocess)} preprocessors to apply for granule {self._resource}')
                    while len(self._preprocess) > 0:
                        preprocessor: GranulePreprocessor = self._preprocess.pop(0)

                        ds = preprocessor.process(ds)

                return ds, granule_name
        except FileNotFoundError:
            raise GranuleLoadingError(f"The granule file {self._resource} does not exist.")
        except Exception as err:
            logger.exception(err)
            raise GranuleLoadingError(f"An error occurred. The granule {self._resource} may not be a valid NetCDF file.")

    @staticmethod
    async def _download_s3_file(url: str):
        parsed_url = parse.urlparse(url)
        logger.info(
            "Downloading S3 file from bucket '{}' with key '{}'".format(parsed_url.hostname, parsed_url.path[1:]))
        async with aioboto3.resource("s3") as s3:
            obj = await s3.Object(bucket_name=parsed_url.hostname, key=parsed_url.path[1:])
            response = await obj.get()
            data = await response['Body'].read()
            logger.info("Finished downloading S3 file.")

        fp = tempfile.NamedTemporaryFile()
        fp.write(data)
        logger.info("Saved downloaded file to {}.".format(fp.name))
        return fp

    @staticmethod
    def _parse_module(module_config: dict):
        module_name = module_config.pop('name')
        try:
            module_class = module_mappings[module_name]
            logger.debug("Loaded preprocessor {}.".format(module_class))
            processor_module = module_class(**module_config)
        except KeyError:
            raise PipelineBuildingError(f"'{module_name}' is not a valid preprocessor.")
        except Exception as e:
            raise PipelineBuildingError(f"Parsing module '{module_name}' failed because of the following error: {e}")

        return processor_module

