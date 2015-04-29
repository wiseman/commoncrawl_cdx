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

import gflags

import commoncrawl_cdx as cdx
from commoncrawl_cdx import cli

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


def get_multi_index_reader(url, indices=None):
    indices = indices or FLAGS.indices
    return cdx.MultiIndexReader(
        url,
        [cdx.index_api_url(c) for c in FLAGS.indices])


def main(argv):
    if len(argv) < 2:
        raise cli.UsageError('You need to specify at least one URL.')
    if not FLAGS.indices:
        raise cli.UsageError(
            'You need to specify at least one index with --indices, e.g. '
            '--indices CC-MAIN-2015-14.  See http://index.commoncrawl.org/ '
            'for a list of indices.')

    for url in argv[1:]:
        index_reader = get_multi_index_reader(url)
        for url, d in index_reader.itemsiter():
            if FLAGS.print_metadata:
                print '%s\t%s' % (url, json.dumps(d))
            else:
                print url


def cli_main():
    cli.App(main=main, usage=inspect.getmodule(main).__doc__).run()

if __name__ == '__main__':
    cli_main()
