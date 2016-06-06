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


class Rib(Task):

    def __init__(self):
        super(Rib, self).__init__()

    def update(self, prefix, attr):
        self.db_connection.collection_name = db_cons.RIB_ATTRIBUTE_EVPN
        db_collection = self.db_connection.get_collection()
        find_ressult = db_collection.find_one(attr)
        if find_ressult:
            attr_id = find_ressult['_id']
        else:
            upsert_result = db_collection.insert_one(attr)
            attr_id = upsert_result.inserted_id
        self.db_connection.collection_name = db_cons.RIB_PREFIX_EVPN
        db_collection = self.db_connection.get_collection()
        db_collection.update_one(
            {'PREFIX': prefix}, {'$set': {'ATTR_ID': attr_id}}, upsert=True)

    def withdraw(self, prefix):
        self.db_connection.collection_name = db_cons.RIB_PREFIX_EVPN
        db_collection = self.db_connection.get_collection()
        db_collection.delete_one(prefix)

    def clear(self):
        for collection in db_cons.RIB_TABLES:
            self.db_connection.collection_name = collection
            self.db_connection.get_collection().remove()