#!/usr/bin/env python

import json
import logging
import multiprocessing.dummy as mp
import urlparse
import zlib

import boto3
import gflags
import requests

logger = logging.getLogger(__name__)
FLAGS = gflags.FLAGS

# The base Common Crawl CDX URL to use.
DEFAULT_CDX_SERVER_URL = 'http://index.commoncrawl.org/'

gflags.DEFINE_integer(
    'num_threads',
    mp.cpu_count(),
    'The number of worker threads to use.')
gflags.DEFINE_string(
    'cdx_server_url',
    'http://index.commoncrawl.org/',
    'The Common Crawl CDX server to use.')
gflags.DEFINE_string(
    'warc_s3_bucket',
    'aws-publicdatasets',
    'The S3 bucket from which WARC files are retrieved.')


class Error(Exception):
    pass


def index_api_url(collection, base_url=None):
    """Constructs the API endipoint URL for an index.

    Each index collection has its own API endpoint. E.g. the
    collection named CC-MAIN-2015 has an API endpoint at
    http://index.commoncrawl.org/CC-MAIN-2015-14-index.

    """
    base_url = base_url or FLAGS.cdx_server_url
    return urlparse.urljoin(base_url, collection + '-index')


class MultiIndexReader(object):
    """Returns results from multiple indices for a given URL."""
    def __init__(self, url, index_api_urls):
        self.index_urls = index_api_urls
        self.readers = [IndexReader(url, s) for s in index_api_urls]

    def _get_reader_page(self, reader_page):
        reader, page = reader_page
        return reader._get_index_page(page)

    def itemsiter(self):
        reader_pages = []
        for reader in self.readers:
            reader_pages += [(reader, p) for p in range(reader._num_pages())]
        if not reader_pages:
            logger.info(
                'No index pages found for url pattern %r in %r',
                self.url, self.index_api_urls)
        pool = mp.Pool(FLAGS.num_threads)
        try:
            for page_results in pool.imap_unordered(
                    self._get_reader_page, reader_pages):
                for r in page_results:
                    yield r['url'], r
        finally:
            pool.close()


class IndexReader(object):
    "Returns result from an index for a given URL."
    def __init__(self, url, index_api_url):
        self.url = url
        self.index_api_url = index_api_url
        logger.info('Opening index reader for url pattern %r at index %r',
                    self.url, self.index_api_url)
        self.num_pages = None

    def _num_pages(self):
        if self.num_pages:
            return self.num_pages
        session = requests.Session()
        r = session.get(
            self.index_api_url,
            params={'url': self.url, 'showNumPages': True})
        r.raise_for_status()
        result = r.json()
        if isinstance(result, dict):
            self.num_pages = result['pages']
        elif isinstance(result, int):
            self.num_pages = result
        else:
            msg = ('Num-pages query for %r at %r returned invalid data: %r' % (
                self.url, self.index_api_url, r.text))
            raise Error(msg)
        return self.num_pages

    def _get_index_page(self, page_num):
        url = self.index_api_url
        logger.info(
            'Getting page %s of %s of results for %r at %r',
            page_num, self.num_pages, self.url, self.index_api_url)
        session = requests.Session()
        r = session.get(
            url,
            params={'url': self.url,
                    'output': 'json',
                    'page': page_num})
        r.raise_for_status()
        lines = r.content.split('\n')
        return [json.loads(l) for l in lines if l]

    def itemsiter(self):
        num_pages = self._num_pages()
        if num_pages == 0:
            logger.info(
                'No index pages found for url pattern %r in %r',
                self.url,
                self.index_api_url)
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


def get_warc_record(index_metadata, bucket=None, keep_compressed=False):
    """Fetches a WARC record from S3."""
    filename = index_metadata['filename']
    range_start = int(index_metadata['offset'])
    range_end = range_start + int(index_metadata['length']) - 1
    bucket = bucket or FLAGS.warc_s3_bucket
    s3 = boto3.client('s3')
    args = {'Bucket': bucket,
            'Key': filename,
            'Range': 'bytes=%s-%s' % (range_start, range_end)}
    logger.info('Fetching %s bytes: %s', index_metadata['length'], args)
    response = s3.get_object(**args)
    streaming_body = response['Body']
    compressed_body = streaming_body.read()
    if keep_compressed:
        return compressed_body
    else:
        # See
        # http://stackoverflow.com/questions/2695152/in-python-how-do-i-decode-gzip-encoding/2695575
        return zlib.decompress(compressed_body, 16 + zlib.MAX_WBITS)
