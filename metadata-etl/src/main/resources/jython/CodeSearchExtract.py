#
# Copyright 2015 LinkedIn Corp. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#

import sys,os,re
import requests
import subprocess
from wherehows.common import Constant
from wherehows.common.schemas import SCMOwnerRecord
from wherehows.common.writers import FileWriter
from org.slf4j import LoggerFactory


class CodeSearchExtract:
    """
    Lists all repos for oracle & espresso databases. Since this feature is not
    available through the UI, we need to use http://go/codesearch to discover
    the multiproduct repos that use 'li-db' plugin.
    """

    # verbose = False
    limit_search_result = 500
    # limit_multiproduct = None
    # limit_plugin = None

    def __init__(self):
        self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)
        self.base_url = args[Constant.BASE_URL_KEY]
        self.code_search_committer_writer = FileWriter(args[Constant.DATABASE_SCM_REPO_OUTPUT_KEY])

    def run(self):
        offset_min = 1
        offset_max = 100
        databases = []
        search_request = \
            {"request":
                {
                    "other":{"CurrentResult":str(offset_min),"requestTimeout":"200000000"},
                    "queryContext":{"numToScore":1000,"docDataSet":"results","rawQuery":"type:gradle plugin:*'li-db'"},
                    "paginationContext":{"numToReturn":offset_max}
                }
            }
        while True:
            resp = requests.post(self.base_url + '/galene-codesearch?action=search',
                                 json=search_request,
                                 verify=False)
            if resp.status_code != 200:
                # This means something went wrong.
                d = resp.json()
                self.logger.info("Request Error! Stack trace {}".format(d['stackTrace']))
                # raise Exception('Request Error', 'POST /galene-codesearch?action=search %s' % (resp.status_code))
                break

            result = resp.json()['value']
            self.logger.debug("Pagination offset = {}".format(result['total']))
            for element in result['elements']:
                fpath = element['docData']['filepath']
                ri = fpath.rindex('/')
                prop_file = fpath[:ri] + '/database.properties'
                # e.g. identity-mt/database/Identity/database.properties
                #      network/database/externmembermap/database.properties
                #      cap-backend/database/campaigns-db/database.properties
                try:
                    databases.append( {'filepath': prop_file, 'app_name': element['docData']['mp']} )
                except:
                    self.logger.error("Exception happens with prop_file {}".format(prop_file))

            if result['total'] < 100:
                break
            offset_min += int(result['total'])
            offset_max += 100 # if result['total'] < 100 else result['total']
            search_request['request']['other']['CurrentResult'] = str(offset_min)
            search_request['request']['paginationContext']['numToReturn'] = offset_max
            self.logger.debug("Property file path {}".format(search_request))

        self.logger.debug(" length of databases is {}".format(len(databases)))

        owner_count = 0
        committers_count = 0
        for db in databases:
            prop_file = db['filepath']
            file_request = \
                {"request":{
                    "other":{"filepath":prop_file,
                             "TextTokenize":"True",
                             "CurrentResult":"1",
                             "requestTimeout":"2000000000"
                             },
                    "queryContext":{"numToScore":10,"docDataSet":"result"},
                    "paginationContext":{"numToReturn":1}
                }
                }
            resp = requests.post(self.base_url + '/galene-codesearch?action=search',
                                 json=file_request,
                                 verify=False)
            if resp.status_code != 200:
                # This means something went wrong.
                d = resp.json()
                self.logger.info("Request Error! Stack trace {}".format(d['stackTrace']))
                continue
            result = resp.json()['value']
            if result['total'] < 1:
                self.logger.info("Nothing found for {}".format(prop_file))
                continue
            if "repoUrl" in result['elements'][0]['docData']:
                db['scm_url'] = result['elements'][0]['docData']['repoUrl']
                db['scm_type'] = result['elements'][0]['docData']['repotype']
                db['committers'] = ''

                if db['scm_type'] == 'SVN':
                    schema_in_repo = re.sub(r"http://(\w+)\.([\w\.\-/].*)database.properties\?view=markup",
                                            "http://svn." + r"\2" + "schema", db['scm_url'])
                    db['committers'] = self.get_svn_committers(schema_in_repo)
                    committers_count +=1
                    self.logger.info("Committers for {} => {}".format(schema_in_repo,db['committers']))

            else:
                self.logger.info("Search request {}".format(prop_file))

            code = result['elements'][0]['docData']['code']
            try:
                code_dict = dict(line.split("=", 1) for line in code.strip().splitlines())

                db['database_name'] = code_dict['database.name']
                db['database_type'] = code_dict['database.type']

                owner_record = SCMOwnerRecord(
                    db['scm_url'],
                    db['database_name'],
                    db['database_type'],
                    db['app_name'],
                    db['filepath'],
                    db['committers'],
                    db['scm_type']
                )
                owner_count += 1
                self.code_search_committer_writer.append(owner_record)
            except Exception as e:
                self.logger.error(e)
                self.logger.error("Exception happens with code {}".format(code))


        self.code_search_committer_writer.close()
        self.logger.info('Finish Fetching committers, total {} committers entries'.format(committers_count))
        self.logger.info('Finish Fetching SVN owners, total {} records'.format(owner_count))


    def get_svn_committers(self, svn_repo_path):
        """Collect recent committers from the cmd
           svn log %s | grep '^\(A=\|r[0-9]* \)' | head -10
           e.g.
           r1617887 | htang | 2016-09-21 14:27:40 -0700 (Wed, 21 Sep 2016) | 12 lines
           A=shanda,pravi
           r1600397 | llu | 2016-08-08 17:14:22 -0700 (Mon, 08 Aug 2016) | 3 lines
           A=rramakri,htang
        """
        #svn_cmd = """svn log %s | grep '^\(A=\|r[0-9]* \)' | head -10"""
        committers = []
        possible_svn_paths = [svn_repo_path, svn_repo_path + "ta"]
        for svn_repo_path in possible_svn_paths:
            p = subprocess.Popen('svn log ' + svn_repo_path + " |grep '^\(A=\|r[0-9]* \)' |head -10",
                                 shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            svn_log_output, svn_log_err = p.communicate()
            if svn_log_err[:12] == 'svn: E160013':
                continue  # try the next possible path

            for line in svn_log_output.split('\n'):
                if re.match(r"r[0-9]+", line):
                    committer = line.split('|')[1].strip()
                    if committer not in committers:
                        committers.append(committer)
                elif line[:2] == 'A=':
                    for apvr in line[2:].split(','):
                        if apvr not in committers:
                            committers.append(apvr)


            if len(committers) > 0:
                self.logger.debug(" {}, ' => ', {}".format(svn_repo_path,committers))
                break

        return ','.join(committers)

if __name__ == "__main__":
    args = sys.argv[1]
    e = CodeSearchExtract()
    e.run()
