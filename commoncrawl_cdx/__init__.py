#!/usr/bin/env python

import json
import logging
import multiprocessing.dummy as mp
import zlib

import boto3
import gflags
import requests

logger = logging.getLogger(__name__)
FLAGS = gflags.FLAGS

gflags.DEFINE_integer(
    'num_threads',
    mp.cpu_count(),
    'The number of worker threads to use.')


# The defaultCommon Crawl CDX index server to use.
DEFAULT_CDX_SERVER = 'http://index.commoncrawl.org/'

# The default index collection.
DEFAULT_COLL = 'CC-MAIN-2015-06'


class Error(Exception):
    pass


def get_default_cdx_server_url():
    return cdx_server_url(DEFAULT_COLL)


def cdx_server_url(collection, server=None):
    server = server or DEFAULT_CDX_SERVER
    return server + collection + '-index'


class MultiIndexReader(object):
    def __init__(self, url, cdx_server_urls):
        print cdx_server_urls
        self.readers = [IndexReader(url, s) for s in cdx_server_urls]

    def _get_reader_page(self, reader_page):
        reader, page = reader_page
        return reader._get_index_page(page)

    def itemsiter(self):
        reader_pages = []
        for reader in self.readers:
            reader_pages += [(reader, p) for p in range(reader._num_pages())]
        if not reader_pages:
            logger.info('No index pages found')
        pool = mp.Pool(FLAGS.num_threads)
        try:
            for page_results in pool.imap_unordered(
                    self._get_reader_page, reader_pages):
                for r in page_results:
                    yield r['url'], r
        finally:
            pool.close()


class IndexReader(object):
    def __init__(self, url, cdx_server_url=None):
        self.url = url
        self.cdx_server_url = cdx_server_url or get_default_cdx_server_url()
        logger.info('Opening index reader for url %r at server %r',
                    self.url, self.cdx_server_url)

    def _num_pages(self):
        session = requests.Session()
        r = session.get(
            self.cdx_server_url,
            params={'url': self.url, 'showNumPages': True})
        result = r.json()
        if isinstance(result, dict):
            return result['pages']
        elif isinstance(result, int):
            return result
        else:
            msg = 'Num pages query returned invalid data: %r' % (r.text,)
            raise Error(msg)

    def _get_index_page(self, page_num):
        url = self.cdx_server_url
        session = requests.Session()
        r = session.get(
            url,
            params={'url': self.url,
                    'output': 'json',
                    'page': page_num})
        lines = r.content.split('\n')
        return [json.loads(l) for l in lines if l]

    def itemsiter(self):
        num_pages = self._num_pages()
        if num_pages == 0:
            logger.info('No index pages found')
        else:
            pool = mp.Pool(FLAGS.num_threads)
            try:
                pages = range(num_pages)
                for page_results in pool.imap_unordered(
                        self._get_index_page, pages):
                    for r in page_results:
                        yield r['url'], r
            finally:
                pool.close()


DEFAULT_S3_BUCKET = 'aws-publicdatasets'


def get_warc_member(filename, bucket=None, range=None):
    bucket = bucket or DEFAULT_S3_BUCKET
    s3 = boto3.client('s3')
    args = {'Bucket': bucket,
            'Key': filename}
    if range:
        range_start = int(range[0])
        range_end = range_start + int(range[1])
        args['Range'] = 'bytes=%s-%s' % (range_start, range_end)
    logger.info('Fetching %s', args)
    response = s3.get_object(**args)
    streaming_body = response['Body']
    compressed_body = streaming_body.read()
    # See http://stackoverflow.com/questions/2695152/in-python-how-do-i-decode-gzip-encoding/2695575
    return zlib.decompress(compressed_body, 16 + zlib.MAX_WBITS)


def open_index_reader(url, cdx_server_url=None):
    return IndexReader(url, cdx_server_url=cdx_server_url)
