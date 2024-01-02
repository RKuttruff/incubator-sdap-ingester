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

import json
import logging
import os
import pathlib
from dataclasses import dataclass
from datetime import datetime, date
from enum import Enum
from fnmatch import fnmatch
from typing import Optional
from urllib.parse import urlparse

from collection_manager.entities.exceptions import MissingValueCollectionError

logger = logging.getLogger(__name__)


class CollectionStorageType(Enum):
    LOCAL = 1
    S3 = 2
    REMOTE = 3
    ZARR = 4


@dataclass(frozen=True)
class Collection:
    dataset_id: str
    projection: str
    dimension_names: frozenset
    slices: frozenset
    path: str
    historical_priority: int
    forward_processing_priority: Optional[int] = None
    date_from: Optional[datetime] = None
    date_to: Optional[datetime] = None
    preprocess: str = None
    processors: str = None
    group: str = None
    store_type: str = None
    config: str = None
    meta: str = None

    @staticmethod
    def __decode_dimension_names(dimension_names_dict):
        """
        - Validating both `variable` and `variables` are not part of the dictionary
        - if it has `variable`, converting it to single element list
        - if it has `variables`, keeping it as a list while renmaing the key to `variable`
        """
        if 'variable' in dimension_names_dict and 'variables' in dimension_names_dict:
            raise RuntimeError('both variable and variables present in dimensionNames. Only one is allowed')
        new_dimension_names = [(k, v) for k, v in dimension_names_dict.items() if k not in ['variable', 'variables']]
        if 'variable' in dimension_names_dict:
            if not isinstance(dimension_names_dict['variable'], (str, dict)):
                raise RuntimeError(f'variable in dimensionNames must be string or a dict: ("name", "cf_standard_name", '
                                   f'"unit") -> string type. value: {dimension_names_dict["variable"]}')

            if isinstance(dimension_names_dict['variable'], dict):
                var_keys = set(dimension_names_dict['variable'].keys())

                if not var_keys == {"name", "cf_standard_name", "unit"}:
                    raise RuntimeError(
                        f'dictionary definition for variable must only contain the keys: "name", "cf_standard_name", '
                        f'"unit" type. value: {dimension_names_dict["variable"]}')

                if not all([isinstance(v, (str, type(None))) for v in dimension_names_dict['variable'].values()]):
                    raise RuntimeError(f'dictionary definition for variable must only contain string or null values. '
                                       f'value: {dimension_names_dict["variable"]}')
            else:
                var_name = dimension_names_dict['variable']
                dimension_names_dict['variable'] = dict(name=var_name, cf_standard_name=None, unit=None)

            new_dimension_names.append(('variable', json.dumps(dimension_names_dict['variable'])))

            return new_dimension_names
        if 'variables' in dimension_names_dict:
            if not isinstance(dimension_names_dict['variables'], list):
                raise RuntimeError(f'variable in dimensionNames must be list type. value: {dimension_names_dict["variables"]}')

            for i in range(len(dimension_names_dict['variables'])):
                v = dimension_names_dict['variables'][i]

                if isinstance(v, str):
                    dimension_names_dict['variables'][i] = dict(name=v, cf_standard_name=None, unit=None)

            new_dimension_names.append(('variable', json.dumps(dimension_names_dict['variables'])))
            return new_dimension_names


    @staticmethod
    def meta_dict(meta, prefix=''):
        new_dict = {}

        def dates(s):
            if isinstance(s, datetime):
                return s.strftime('%Y-%m-%dT%H:%M:%S%z')
            elif isinstance(s, date):
                return s.strftime('%Y-%m-%d')
            else:
                return s

        for key in meta:
            new_key = key if prefix == '' else f'{prefix}.{key}'
            value = meta[key]

            if new_key == 'coverage':
                try:
                    start = dates(value['dateStart'])
                    stop = dates(value['dateStop'])
                    bbox = value['bbox']

                    new_dict[new_key] = f'Temporal range: {start} to {stop} | Spatial range: {bbox}'
                    continue
                except:
                    pass

            if isinstance(value, dict):
                new_dict.update(Collection.meta_dict(value, new_key))
            else:
                new_dict[new_key] = repr(dates(value))

        if prefix == '':
            return json.dumps(new_dict)
        else:
            return new_dict.items()


    @staticmethod
    def from_dict(properties: dict):
        """
        Accepting either `variable` or `variables` from the configmap
        """
        logger.debug(f'incoming properties dict: {properties}')
        try:
            date_to = datetime.fromisoformat(properties['to']) if 'to' in properties else None
            date_from = datetime.fromisoformat(properties['from']) if 'from' in properties else None

            store_type = properties.get('storeType')

            slices = properties.get('slices', {})

            preprocess = json.dumps(properties['preprocess']) if 'preprocess' in properties else None
            extra_processors = json.dumps(properties['processors']) if 'processors' in properties else None
            config = properties['config'] if 'config' in properties else None

            projection = properties['projection'] if 'projection' in properties else None

            meta = Collection.meta_dict(properties['meta']) if 'meta' in properties else None

            collection = Collection(dataset_id=properties['id'],
                                    projection=projection,
                                    dimension_names=frozenset(Collection.__decode_dimension_names(properties['dimensionNames'])),
                                    slices=frozenset(slices.items()),
                                    path=properties['path'],
                                    historical_priority=properties['priority'],
                                    forward_processing_priority=properties.get('forward-processing-priority', None),
                                    date_to=date_to,
                                    date_from=date_from,
                                    preprocess=preprocess,
                                    processors=extra_processors,
                                    group=properties.get('group'),
                                    store_type=store_type,
                                    config=config,
                                    meta=meta
                                    )
            return collection
        except KeyError as e:
            raise MissingValueCollectionError(missing_value=e.args[0])

    def storage_type(self):
        if self.store_type == 'zarr':
            return CollectionStorageType.ZARR
        if urlparse(self.path).scheme == 's3':
            return CollectionStorageType.S3
        elif urlparse(self.path).scheme in {'http', 'https'}:
            return CollectionStorageType.REMOTE
        else:
            return CollectionStorageType.LOCAL

    def directory(self):
        if urlparse(self.path).scheme == 's3':
            return self.path
        elif os.path.isdir(self.path):
            return self.path
        else:
            return os.path.dirname(self.path)

    def owns_file(self, file_path: str) -> bool:
        if urlparse(file_path).scheme == 's3':
            return file_path.find(self.path) == 0
        else:
            if os.path.isdir(file_path):
                raise IsADirectoryError()

            if os.path.isdir(self.path):
                return pathlib.Path(self.path) in pathlib.Path(file_path).parents
            else:
                return fnmatch(file_path, self.path)
