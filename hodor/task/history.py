# Copyright 2016 Cisco Systems, Inc.
# All rights reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

from hodor.task import Task
from hodor.db import constants as db_cons

class History(Task):

    def __init__(self):
        super(History, self).__init__()
        self.db_connection.collection_name = db_cons.RIB_HISTORY_EVPN

    def insert(self, prefix, attr, withdraw=False):
        record = {
            'attr': attr,
            'prefix': prefix,
            'action': 'update' if withdraw is False else 'withdraw'
        }
        self.db_connection.get_collection().insert_one(record)
