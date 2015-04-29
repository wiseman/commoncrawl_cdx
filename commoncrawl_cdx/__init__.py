#!/usr/bin/env python

import json
import logging
import multiprocessing.dummy as mp

import requests


logger = logging.getLogger(__name__)


DEFAULT_CDX_SERVER = 'http://index.commoncrawl.org/'
DEFAULT_COLL = 'CC-MAIN-2015-06'


class Error(Exception):
    pass


def get_default_cdx_server_url():
    return DEFAULT_CDX_SERVER + DEFAULT_COLL + '-index'


class IndexReader(object):
    def __init__(self, url, cdx_server_url=None):
        self.url = url
        self.cdx_server_url = cdx_server_url or get_default_cdx_server_url()
        logger.info('Opening index reader for url %r at server %r',
                    self.url, self.cdx_server_url)

    def num_pages(self):
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
        num_pages = self.num_pages()
        if num_pages == 0:
            logger.info('No index pages found')
        else:
            pool = mp.Pool()
            try:
                pages = range(num_pages)
                for page_results in pool.imap_unordered(
                        self._get_index_page, pages):
                    for r in page_results:
                        yield r['url'], r
            finally:
                pool.close()


def open_index_reader(url, cdx_server_url=None):
    return IndexReader(url, cdx_server_url=cdx_server_url)
