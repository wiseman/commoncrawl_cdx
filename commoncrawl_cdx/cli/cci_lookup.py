"""Prints a list of URLS from the Common Crawl Index that match a
prefix.

Usage:
  %s [options] <URL pattern>...

Examples:

  %s google.com
  %s http://google.com/
  %s google.com/maps/*
"""

import inspect
import json
import logging
import multiprocessing.dummy as mp
import sys
import threading

import gflags

import commoncrawl_cdx as cdx
from commoncrawl_cdx import cli

logger = logging.getLogger(__name__)
FLAGS = gflags.FLAGS

gflags.DEFINE_boolean(
    'print_metadata',
    False,
    ('Print metadata for each URL. Metadata is in JSON format and is '
     'separated from the URL with a tab character.'),
    short_name='m')

gflags.DEFINE_list(
    'indices',
    [],
    ('A list of indices/collections to use, e.g. --index '
     'CC-MAIN-2015-06,CC-MAIN-2015-14.'),
    short_name='i')

gflags.DEFINE_boolean(
    'fetch_content',
    False,
    'If true, fetch the contents of matching URLs.')


def get_multi_index_reader(url, indices=None):
    indices = indices or FLAGS.indices
    return cdx.MultiIndexReader(
        url,
        [cdx.index_api_url(c) for c in FLAGS.indices])


stdout_lock = threading.Lock()


def write_content(content):
    with stdout_lock:
        sys.stdout.write(content)


class Memo(object):
    def __init__(self):
        self.lock = threading.Lock()
        self.memo = {}

    def check_and_add(self, url, digest):
        with self.lock:
            if url in self.memo or digest in self.memo:
                r = True
            else:
                r = False
            self.memo[url] = True
            self.memo[digest] = True
        return r


already_fetched = Memo()


def fetch_content(metadata):
    try:
        content_key = metadata['urlkey']
        digest = metadata['digest']
        if already_fetched.check_and_add(content_key, digest):
            logger.info('Skipping %r', metadata['url'])
        else:
            logger.info('Fetching %r', metadata['url'])
            content = cdx.get_warc_record(metadata)
            write_content(content)
    except:
        logger.exception('Error while fetching %r', metadata)
        raise


def main(argv):
    if len(argv) < 2:
        raise cli.UsageError('You need to specify at least one URL.')
    if not FLAGS.indices:
        raise cli.UsageError(
            'You need to specify at least one index with --indices, e.g. '
            '--indices CC-MAIN-2015-14.  See http://index.commoncrawl.org/ '
            'for a list of indices.')
    pool = mp.Pool()
    for url in argv[1:]:
        index_reader = get_multi_index_reader(url)
        for url, d in index_reader.itemsiter():
            if FLAGS.fetch_content:
                pool.apply_async(fetch_content, [d])
            else:
                if FLAGS.print_metadata:
                    print '%s\t%s' % (url, json.dumps(d))
                else:
                    print url
    pool.close()
    logger.info('Waiting on pool')
    pool.join()


def cli_main():
    cli.App(main=main, usage=inspect.getmodule(main).__doc__).run()

if __name__ == '__main__':
    cli_main()
