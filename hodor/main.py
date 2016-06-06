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

import os
import traceback
import sys
import logging
from oslo_config import cfg

from hodor.consumer import Consumer
from hodor.db import config as db_config
from hodor import config as basic_config
from hodor import version, log

log.early_init_log(logging.DEBUG)

CONF = cfg.CONF

LOG = logging.getLogger(__name__)


def prepare(args=None):
    """
    :param args:
    :return:
    """
    db_config.register_options()
    basic_config.register_options()
    try:
        CONF(args=args, project='hodor', version=version,
             default_config_files=['/etc/hodor/hodor.ini'])
    except cfg.ConfigFilesNotFoundError:
        CONF(args=args, project='hodor', version=version)
    log.init_log()
    LOG.info('Log (Re)opened.')
    LOG.info("Configuration:")
    cfg.CONF.log_opt_values(LOG, logging.INFO)

    # write pid file
    if CONF.pid_file:
        with open(CONF.pid_file, 'w') as pid_file:
            pid_file.write(str(os.getpid()))
            LOG.info('create pid file: %s' % CONF.pid_file)

    # check message file path and peer ip address
    if not os.path.exists(CONF.message.dir):
        LOG.error('Message path %s does not exist!', os.path.join(CONF.message.dir))
        sys.exit()


def main():
    consumer = None
    try:
        prepare()
        consumer = Consumer(seq=0)
        consumer.start()
    except Exception as e:
        print traceback.format_exc()
        if consumer:
            consumer.stop()
    except KeyboardInterrupt:
        if consumer:
            consumer.stop()