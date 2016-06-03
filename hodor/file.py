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

"""Message File Operator"""

import os
import time
import sys
import logging
import traceback
import json

LOG = logging.getLogger(__name__)


class MessageFileOperator(object):

    def __init__(self, msgfile_dir, lastseq=0, file_index=-1):
        """
        Get the file we wanted and located the line according to the sequence number.
        line of it.
        init message file object
        :param msgfile_dir: message file dir for this peer
        :param lastseq: last sequence number that read successfully, if `lastseq` = 0, then read
        from the first line of the first file, if `lastseq` = -1, then read from the first line from
        the last updated file. otherwise, try to find the exactly line of sequence number.
        :param file_index: locate file based its index
        """
        self.file_dir = msgfile_dir
        self.lastseq = lastseq
        self.file_index = file_index
        self.file_name = None
        self.last_line = ''
        self._f = None
        self.first_time_catchup_flag = False

    def locate_file_and_line(self):
        self.file_name = self._locate_file()
        self._f = self._locate_line(self.file_name, self.lastseq)

    def _locate_file(self):
        """
        locate the right message file
        """
        file_name = None
        LOG.info('Locate message file, when message seq = %s.' % self.lastseq)
        if not os.path.exists(self.file_dir):
            LOG.critical('The BGP data path does not exist, path=%s' % self.file_dir)
            sys.exit()
        self.file_list = os.listdir(self.file_dir)
        # delete dir
        dir_name_list = []
        for file_ in self.file_list:
            if os.path.isdir(os.path.join(self.file_dir, file_)):
                dir_name_list.append(file_)
        for file_ in dir_name_list:
            self.file_list.remove(file_)
        if not self.file_list:
            LOG.critical('There are no files!')
            sys.exit()
        self.file_list.sort()

        if self.lastseq == 0:
            # return the first file
            file_name = os.path.join(self.file_dir, self.file_list[0])
            LOG.info('Locate file successfully, file = %s' % file_name)
            return file_name

        elif self.lastseq == -1:
            # return the last file
            file_name = os.path.join(self.file_dir, self.file_list[-1])
            LOG.info('Locate file successfully, file = %s' % file_name)
            return file_name

        # return the file contain the sequence number
        find_flag = False
        for file_ in self.file_list:
            # get the first line and last line, if sequence number is between the first line
            # and last line, then find_flag is True and return the file name
            file_name = os.path.join(self.file_dir, file_)
            first_line = {}
            last_line = {}
            with open(file_name, 'r') as f:
                t = 0
                while True:
                    first_line = next(f)
                    try:
                        first_line = json.loads(first_line)
                        break

                    except Exception as e:
                        LOG.error(e)
                        ex_str = traceback.format_exc()
                        LOG.error(ex_str)
                        time.sleep(120)
                        t += 1
                        if t > 10:
                            LOG.critical("Can not get first line of %s" % file_name)
                            sys.exit()
                offs = -100
                while True:
                    f.seek(offs, 2)
                    lines = f.readlines()
                    if len(lines) > 1:
                        try:
                            last_line = lines[-1]
                            last_line = json.loads(last_line)
                        except Exception, e:
                            LOG.exception(e.message)
                            last_line = lines[-2]
                            try:
                                last_line = json.loads(last_line)
                            except Exception as e:
                                LOG.error(e)
                                ex_str = traceback.format_exc()
                                LOG.error(ex_str)
                                LOG.critical("Can not get the last line of %s" % file_name)
                                sys.exit()
                        break
                    offs *= 2
            if first_line and last_line:
                if first_line['seq'] <= self.lastseq <= last_line['seq']:
                    find_flag = True
                    break
        if find_flag:
            LOG.info('Locate file successfully, file = %s' % file_name)
            return os.path.join(self.file_dir, file_name)
        else:
            LOG.critical('Can not locate message file, when seq=%s' % self.lastseq)
            sys.exit()

    @staticmethod
    def _locate_line(msgfile, lastseq=0):
        """Get bgp message file handles and seek to the position after seq number your input
        """

        # Open message file
        f = open(msgfile, "r")
        LOG.info('Open BGP message file: %s' % msgfile)
        if lastseq in [0, -1]:
            return f
        else:  # skip lastseq
            line = f.readline()
            while line:
                line_json = json.loads(line)
                if line_json['seq'] == lastseq:
                    break
                line = f.readline()
        return f

    @property
    def get_next_file(self):

        old_file_name = os.path.split(self.file_name)[-1]
        file_list = os.listdir(self.file_dir)
        # delete dirs
        dir_name_list = []
        for file_ in file_list:
            if os.path.isdir(os.path.join(self.file_dir, file_)):
                dir_name_list.append(file_)
        for file_ in dir_name_list:
            file_list.remove(file_)
        file_list.sort()
        if len(file_list) == 1:
            return None
        index = file_list.index(old_file_name)
        if index + 1 == len(file_list):
            return None
        try:
            new_file_name = file_list[index + 1]
        except Exception as e:
            LOG.error(e)
            ex_str = traceback.format_exc()
            LOG.error(ex_str)
            return None
        return os.path.join(self.file_dir, new_file_name)

    @property
    def readline(self):
        """
        read one line and return
        return None if no new line
        """

        line = self._f.readline()
        if line and line.endswith('\n'):
            self.last_line = line
            return line
        elif line:  # this line is still be writting
            last_len = len(line)
            self._f.seek(self._f.tell() - last_len)
            self.first_time_catchup_flag = True
            return None
        else:
            if not self.first_time_catchup_flag:
                self.first_time_catchup_flag = True
                LOG.info('first time catchup the file ending')
                return None
            time.sleep(1)
            # if need to open the next new file
            next_file = self.get_next_file
            if next_file:
                # try old file again
                line = self._f.readline()
                if line and line.endswith('\n'):
                    self.last_line = line
                    return line
                elif line:
                    last_len = len(line)
                    self._f.seek(self._f.tell() - last_len)
                    return None
                else:
                    # need to check the next file
                    with open(next_file, 'r') as f_next:
                        first_line = f_next.readline()
                        try:
                            first_seq = json.loads(first_line)['seq']
                            last_seq = json.loads(self.last_line)['seq']
                            LOG.debug('first seq of next file=%s, last seq of old file=%s' % (first_seq, last_seq))
                            if last_seq + 1 == first_seq:
                                # really need open next file
                                self._f.close()  # close old file
                                self._f = open(next_file, 'r')
                                LOG.info('Open next BGP message file: %s' % next_file)
                                self.file_name = next_file
                                return None
                            else:
                                f_next.close()
                                return None
                        except Exception as e:
                            LOG.error(e)
                            error_str = traceback.format_exc()
                            LOG.debug(error_str)
                            return None
            else:
                return None
