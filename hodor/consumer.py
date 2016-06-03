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

import json
import traceback
import logging
import time

from oslo_config import cfg

from hodor.file import MessageFileOperator
from hodor import constants as hold_cons
from hodor.task.history import History
from hodor.task.rib import Rib
from hodor.db.mongo import MongoOpt

LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class Consumer(object):

    def __init__(self):

        self.db_connection = MongoOpt(connection_url=CONF.database.url, db_name=CONF.database.name)

    def start(self):

        file_handler = MessageFileOperator(CONF.message.dir, lastseq=-1)
        file_handler.locate_file_and_line()
        while True:
            line = file_handler.readline
            if not line:
                time.sleep(2)
                continue
            try:
                bgp_msg = json.loads(line)
            except Exception as e:
                LOG.debug(traceback.format_exc())
                LOG.critical('Message format error when using json.loads(line), line = %s detail: %s' % (line, e))
                continue
            if bgp_msg['type'] == hold_cons.BGP_UPDATE:
                try:
                    if bgp_msg['msg'].get('afi_safi') != 'evpn':
                        print bgp_msg
                except Exception as e:
                    LOG.error(e)
                    LOG.debug(traceback.format_exc())
            elif bgp_msg['type'] in [hold_cons.BGP_NOTIFICATION, hold_cons.BGP_OPEN]:
                pass

            else:
                pass

    def stop(self):
        pass
