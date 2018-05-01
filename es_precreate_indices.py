#!/usr/bin/env python

from elasticsearch import Elasticsearch
import datetime
import re
import time
import argparse

"""
Script for elastcisearch indices creation, based on existing indices.
"""

__author__ = "Adel Sachkov <adel.sachkov@yandex.ru>"
__date__ = "1 May 2018"
__version__ = "$Revision: 1.0 $"

es_host = 'localhost'
es_port = 9200
search_pattern = r"(.*\-logs\-)\d\d\d\d\.\d\d\.\d\d"
today = datetime.date.today().strftime('%Y.%m.%d')
tomorrow = (datetime.date.today() + datetime.timedelta(days=1)).strftime('%Y.%m.%d')
delay = 5   # Delay between indices creation (sec.)

if __name__ == '__main__':

    create = False
    parser = argparse.ArgumentParser(description='Script for pre-creating indices for next day.')
    parser.add_argument('--create', nargs='?', default='dry-run', help='Performs index creation (default: false, dry-run only)')
    args = parser.parse_args()

    if not args.create:
        create = True
        print "Dry-run mode disabled. Performing indexes creation..."

    # Establishing a connection to Elasticsearch
    es = None
    try:
        es = Elasticsearch(host=es_host, port=es_port, timeout=30)
    except Exception as e:
        print "Elasticsearch connection error.", e
        exit(1)

    indices = es.indices.get('*-logs-' + today)
    for index in sorted(indices.keys()):
        print "Index:", index

        index_body = {'mappings': indices[index]['mappings']}
        print "Settings:", index_body

        match = re.search(search_pattern, index)
        index_tomorrow = ''.join(match.groups()) + tomorrow

        if create:
            print "Creating index for tomorrow:", index_tomorrow
            es.indices.create(index=index_tomorrow, ignore=400, body=index_body)
            time.sleep(5)
        else:
            print "Dry-run. Index need to be created:", index_tomorrow
