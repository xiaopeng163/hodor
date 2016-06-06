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

LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class Consumer(object):

    def __init__(self, seq):

        self.rib_hander = Rib()
        self.history_hander = History()
        self.seq = seq

    def start(self):
        file_handler = MessageFileOperator(CONF.message.dir, lastseq=self.seq)
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
                        LOG.warning('not evpn message %s', bgp_msg['msg'])

                    # for evpn update
                    mpreach_nlri = bgp_msg['msg']['attr'].get('14')
                    if mpreach_nlri:
                        bgp_msg['msg']['attr'].pop('14')
                        bgp_msg['msg']['attr']['3'] = mpreach_nlri['nexthop']

                        for nlri in mpreach_nlri['nlri']:
                            if nlri['type'] == 2:
                                nlri['value'].pop('label')
                                self.rib_hander.update(prefix=nlri['value'], attr=bgp_msg['msg']['attr'])
                                self.history_hander.insert(prefix=nlri['value'], attr=bgp_msg['msg']['attr'])
                    mpunreach_nlri = bgp_msg['msg']['attr'].get('15')
                    if mpunreach_nlri:
                        for withdraw in mpunreach_nlri['withdraw']:
                            if withdraw['type'] == 2:
                                withdraw['value'].pop('label')
                                self.rib_hander.withdraw(prefix=withdraw['value'])
                                self.history_hander.insert(prefix=withdraw['value'], attr={}, withdraw=True)
                except Exception as e:
                    LOG.error(e)
                    LOG.debug(traceback.format_exc())
            elif bgp_msg['type'] in [hold_cons.BGP_NOTIFICATION, hold_cons.BGP_OPEN]:
                self.rib_hander.clear()

            else:
                pass

    def stop(self):
        pass
