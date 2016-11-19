# -*- coding: utf-8 -*-
#
# Copyright (c) 2016 -- Lars Heuer - Semagia <http://www.semagia.com/>.
# All rights reserved.
#
# License: Public Domain
#
"""\
Dumps the content of the `NARA AAD <https://aad.archives.gov/aad/>`_ electronic
telegram / cables overview table into CSV files.

The CSV files provide a download link for each cable, the cable identifier and
other meta data like sender, recpient and cable date.
"""
from __future__ import absolute_import, unicode_literals
import re
from functools import partial
import csv
import time
import requests
import lxml
import lxml.html
try:
    # Py3
    from urllib.parse import urljoin
except ImportError:
    # Py2
    from urlparse import urljoin
    from io import open
    from backports import csv


_MULTIPLE_WS_PATTERN = re.compile(r'([ ]{2,})')
_NEXT_PAGE_PATTERN = re.compile(r'<a\s+href=[\'"]([^\'"]+)[\'"].*?>Next.*?</a>')
_META_REFRESH_PATTERN = re.compile(r'http-equiv\=[\'"]refresh[\'"]', re.IGNORECASE)

_START_BASE_URL = 'https://aad.archives.gov/aad/display-partial-records.jsp?cat=TS17&tf=X&bc=%2Csl%2Cfd&q=&btnSearch=Search&as_alq=&as_anq=&as_epq=&as_woq='

_YEAR_TO_STARTURL = {
    1973: _START_BASE_URL + '&dt=2472&sc=25993%2C25962%2C25986%2C25942%2C25958%2C25973%2C25959%2C25946&nfo_25993=D%2C0%2C1900&op_25993=7&txt_25993=01%2F01%2F1973&txt_25993=&nfo_25962=V%2C0%2C1900&op_25962=0&txt_25962=&nfo_25986=V%2C0%2C1900&op_25986=0&txt_25986=&nfo_25942=V%2C0%2C1900&op_25942=0&txt_25942=&nfo_25958=V%2C100%2C1900&op_25958=0&txt_25958=&nfo_25973=V%2C0%2C1900&cl_25973=&nfo_25959=V%2C0%2C1900&op_25959=0&txt_25959=&nfo_25946=V%2C4000%2C1900&op_25946=0&txt_25946=&rpp=50',
    1974: _START_BASE_URL + '&dt=2474&sc=25993%2C25962%2C25986%2C25942%2C25958%2C25973%2C25959%2C25946&nfo_25993=D%2C0%2C1900&op_25993=7&txt_25993=01%2F01%2F1974&txt_25993=&nfo_25962=V%2C0%2C1900&op_25962=0&txt_25962=&nfo_25986=V%2C0%2C1900&op_25986=0&txt_25986=&nfo_25942=V%2C0%2C1900&op_25942=0&txt_25942=&nfo_25958=V%2C100%2C1900&op_25958=0&txt_25958=&nfo_25973=V%2C0%2C1900&cl_25973=&nfo_25959=V%2C0%2C1900&op_25959=0&txt_25959=&nfo_25946=V%2C4000%2C1900&op_25946=0&txt_25946=&rpp=50',
    1975: _START_BASE_URL + '&dt=2476&sc=25993%2C25962%2C25986%2C25942%2C25958%2C25973%2C25959%2C25946&nfo_25993=D%2C0%2C1900&op_25993=7&txt_25993=01%2F01%2F1975&txt_25993=&nfo_25962=V%2C0%2C1900&op_25962=0&txt_25962=&nfo_25986=V%2C0%2C1900&op_25986=0&txt_25986=&nfo_25942=V%2C0%2C1900&op_25942=0&txt_25942=&nfo_25958=V%2C100%2C1900&op_25958=0&txt_25958=&nfo_25973=V%2C0%2C1900&cl_25973=&nfo_25959=V%2C0%2C1900&op_25959=0&txt_25959=&nfo_25946=V%2C4000%2C1900&op_25946=0&txt_25946=&rpp=50',
    1976: _START_BASE_URL + '&dt=2082&sc=25993%2C25962%2C25986%2C25942%2C25958%2C25973%2C25959%2C25946&nfo_25993=D%2C0%2C1900&op_25993=7&txt_25993=01%2F01%2F1976&txt_25993=&nfo_25962=V%2C0%2C1900&op_25962=0&txt_25962=&nfo_25986=V%2C0%2C1900&op_25986=0&txt_25986=&nfo_25942=V%2C0%2C1900&op_25942=0&txt_25942=&nfo_25958=V%2C100%2C1900&op_25958=0&txt_25958=&nfo_25973=V%2C0%2C1900&cl_25973=&nfo_25959=V%2C0%2C1900&op_25959=0&txt_25959=&nfo_25946=V%2C4000%2C1900&op_25946=0&txt_25946=&rpp=50',
    1977: _START_BASE_URL + '&dt=2532&sc=27552%2C27521%2C27545%2C27501%2C27517%2C27532%2C27518%2C27505&nfo_27552=D%2C0%2C1900&op_27552=7&txt_27552=01%2F01%2F1977&txt_27552=&nfo_27521=V%2C0%2C1900&op_27521=0&txt_27521=&nfo_27545=V%2C0%2C1900&op_27545=0&txt_27545=&nfo_27501=V%2C0%2C1900&op_27501=0&txt_27501=&nfo_27517=V%2C100%2C1900&op_27517=0&txt_27517=&nfo_27532=V%2C0%2C1900&cl_27532=&nfo_27518=V%2C0%2C1900&op_27518=0&txt_27518=&nfo_27505=V%2C4000%2C1900&op_27505=0&txt_27505=&rpp=50',
    1978: _START_BASE_URL + '&dt=2694&sc=28761%2C28733%2C28754%2C28715%2C28729%2C28741%2C28730%2C28719&nfo_28761=D%2C0%2C1900&op_28761=7&txt_28761=01%2F01%2F1978&txt_28761=&nfo_28733=V%2C0%2C1900&op_28733=0&txt_28733=&nfo_28754=V%2C0%2C1900&op_28754=0&txt_28754=&nfo_28715=V%2C0%2C1900&op_28715=0&txt_28715=&nfo_28729=V%2C100%2C1900&op_28729=0&txt_28729=&nfo_28741=V%2C0%2C1900&cl_28741=&nfo_28730=V%2C0%2C1900&op_28730=0&txt_28730=&nfo_28719=V%2C4000%2C1900&op_28719=0&txt_28719=&rpp=50',
    1979: _START_BASE_URL + '&dt=2776&sc=29222%2C29194%2C29215%2C29176%2C29190%2C29202%2C29191%2C29180&nfo_29222=D%2C0%2C1900&op_29222=7&txt_29222=01%2F01%2F1979&txt_29222=&nfo_29194=V%2C0%2C1900&op_29194=0&txt_29194=&nfo_29215=V%2C0%2C1900&op_29215=0&txt_29215=&nfo_29176=V%2C0%2C1900&op_29176=0&txt_29176=&nfo_29190=V%2C100%2C1900&op_29190=0&txt_29190=&nfo_29202=V%2C0%2C1900&cl_29202=&nfo_29191=V%2C0%2C1900&op_29191=0&txt_29191=&nfo_29180=V%2C4000%2C1900&op_29180=0&txt_29180=&rpp=50',
}

_MONTHS = ('JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC')

_DEFAULT_USER_AGENT = 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.1'


def _make_session(useragent=None):
    """\
    Creates a requests.Session.
    """
    sess = requests.session()
    sess.headers.update({'User-Agent': (useragent or _DEFAULT_USER_AGENT)})
    return sess


def _get_page(sess, url):
    """\
    Returns the HTML content as string.

    :param sess: Session
    :param str url: The URL.
    :raises: requests.exceptions.HTTPError
    """
    page = None
    while True:
        res = sess.get(url)
        if res.status_code == 500:
            time.sleep(5)
            res = sess.get(url)
        res.raise_for_status()
        page = res.content
        if not _META_REFRESH_PATTERN.search(page):
            break
        time.sleep(1)
    return page


def make_csv(year, out, csv_header=True, useragent=None):
    """\
    Creates the CSV for the provided year.

    :param int year: The year.
    :param str out: Path to the CSV file.
    :param bool csv_header: Indicates if a CSV header should be written.
    :param str useragent: The user agent string to use (``None`` indicates a
            default user agent string)
    """
    def _remove_multiple_ws(s):
        """\
        Folds two or more space characters into one.
        """
        return _MULTIPLE_WS_PATTERN.sub(' ', s)

    def _next_page_url(html, base_url):
        """\
        Returns the link to the next page.
        """
        m = _NEXT_PAGE_PATTERN.search(html)
        if not m:
            return None
        return urljoin(base_url, m.group(1))

    def _html_table_row_iter(page):
        """\
        Returns an iterator over the query result rows.
        """
        el = lxml.html.fromstring(page)
        for row in el.xpath('//table[@id="queryResults"]/tbody/tr'):
            children = row.xpath('td')[:-1]
            res_row = [children[0].xpath('a/@href')[0]]
            res_row.extend([child.text for child in children[1:]])
            yield res_row

    sess = _make_session(useragent)
    get_html = partial(_get_page, sess)
    url = _YEAR_TO_STARTURL[year]
    f = open(out, 'w', newline='', encoding='utf-8')
    writer = csv.writer(f)
    if csv_header:
        writer.writerow(['URL', 'Draft Date', 'Document Number', 'Film Number', 'From', 'Subject', 'TAGS', 'To'])
    writerow = writer.writerow
    html = get_html(url)
    while html:
        for doc_url, date, doc_no, film_no, sender, subject, tags, recipient in _html_table_row_iter(html):
            subject = _remove_multiple_ws(subject)
            doc_url = urljoin(url, doc_url)
            sender = sender or ''
            tags = tags or ''
            recipient = recipient or ''
            row = [doc_url, date, doc_no, film_no, sender, subject, tags, recipient]
            writerow(row)
        url = _next_page_url(html, url)
        html = get_html(url) if url is not None else None
    f.flush()
    f.close()
