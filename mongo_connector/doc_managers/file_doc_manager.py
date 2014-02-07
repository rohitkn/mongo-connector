# Copyright 2012 10gen, Inc.
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
# See the License for the specific language governing permissions and
# limitations under the License.

# This file will be used with PyPi in order to package and distribute the final
# product.

"""Receives documents from the oplog worker threads and indexes them
into the backend.

This file is a document manager that implements the doc manager *interface*
methods sending the notification it receives as insert, upsert, search to an
http url like http://localhost:8080/transform/test
It is up to the servlet / handler on the http server to do process the request
"""
import sys
from threading import Timer

from bson.json_util import dumps


class DocManager():
    """The DocManager class creates a connection to the backend engine and
    adds/removes documents, and in the case of rollback, searches for them.

    The reason for storing id/doc pairs as opposed to doc's is so that multiple
    updates to the same doc reflect the most up to date version as opposed to
    multiple, slightly different versions of a doc.
    """

    def __init__(self, url, auto_commit=True, unique_key='_id',  **kwargs):
        """Verify Solr URL and establish a connection.
        """
        self.unique_key = unique_key
        self.auto_commit = auto_commit
        if url is not None:
            print "url: ", url
            self.url = open(url, "a")
        else:
            self.url = sys.stdout
        
        if auto_commit:
            self.run_auto_commit()

    def stop(self):
        self.auto_commit = False

    def upsert(self, doc):
        """Update or insert a document into Solr

        This method should call whatever add/insert/update method exists for
        the backend engine and add the document in there. The input will
        always be one mongo document, represented as a Python dictionary.
        """
        strdoc = dumps(doc)
        self.url.write(strdoc)
        self.url.flush()
    def remove(self, doc):
        """Removes documents from Solr

        The input is a python dictionary that represents a mongo document.
        """
        id = str(doc[self.unique_key])
        """self.url.write("remove", "{ _id: '%s'}"%(id))"""
        """print "remove {_id:",(id),"}"""
        print dumps(doc)
    def search(self, start_ts, end_ts):
        """Called to query Solr for documents in a time range.
        """
        query = '{_ts: [%s TO %s]}' % (start_ts, end_ts)
        self.url.write("query", query)
        print query
    def commit(self):
        """Simply passes since we're not using an engine that needs commiting.
        """
        self.url.flush()

    def run_auto_commit(self):
        """Periodically commits to the Solr server.
        """
        if self.auto_commit:
            Timer(1000, self.run_auto_commit).start()

    def get_last_doc(self):
        """Returns the last document stored in the Solr engine.
        """
        #search everything, sort by descending timestamp, return 1 row
        result = self.search('*:*', sort='_ts desc', rows=1)

        if len(result) == 0:
            return None

        return result.docs[0]