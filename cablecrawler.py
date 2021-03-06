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
import os
import glob
from functools import partial
import csv
import time
import requests
import lxml.html
try:
    # Py3
    from urllib.parse import urljoin
except ImportError:
    # Py2
    from urlparse import urljoin
    range = xrange
    from io import open
    from backports import csv


_MULTIPLE_WS_PATTERN = re.compile(r'([ ]{2,})')
_NEXT_PAGE_PATTERN = re.compile(r'<a\s+href=[\'"]([^\'"]+)[\'"].*?>Next.*?</a>')
_META_REFRESH_PATTERN = re.compile(r'http-equiv\=[\'"]refresh[\'"]', re.IGNORECASE)
_AAD_FILENAME_PATTERN = re.compile(r'filename=(.+)')
_MONTH_PATTERN = re.compile(r'\s*[0-9]+[ \-]+([A-Za-z]+)')
_PG_PATTERN = re.compile(r'[\?&]pg=([0-9]+)')

_START_BASE_URL = 'https://aad.archives.gov/aad/display-partial-records.jsp?cat=TS17&tf=X&bc=%2Csl%2Cfd&q=&btnSearch=Search&as_alq=&as_anq=&as_epq=&as_woq='

_YEAR_TO_STARTURL = {
    1973: _START_BASE_URL + '&dt=2472&sc=25993%2C25962%2C25986%2C25942%2C25958%2C25973%2C25959%2C25946&nfo_25993=D%2C0%2C1900&op_25993=7&txt_25993=01%2F01%2F1973&txt_25993=&nfo_25962=V%2C0%2C1900&op_25962=0&txt_25962=&nfo_25986=V%2C0%2C1900&op_25986=0&txt_25986=&nfo_25942=V%2C0%2C1900&op_25942=0&txt_25942=&nfo_25958=V%2C100%2C1900&op_25958=0&txt_25958=&nfo_25973=V%2C0%2C1900&cl_25973=&nfo_25959=V%2C0%2C1900&op_25959=0&txt_25959=&nfo_25946=V%2C4000%2C1900&op_25946=0&txt_25946=',
    1974: _START_BASE_URL + '&dt=2474&sc=25993%2C25962%2C25986%2C25942%2C25958%2C25973%2C25959%2C25946&nfo_25993=D%2C0%2C1900&op_25993=7&txt_25993=01%2F01%2F1974&txt_25993=&nfo_25962=V%2C0%2C1900&op_25962=0&txt_25962=&nfo_25986=V%2C0%2C1900&op_25986=0&txt_25986=&nfo_25942=V%2C0%2C1900&op_25942=0&txt_25942=&nfo_25958=V%2C100%2C1900&op_25958=0&txt_25958=&nfo_25973=V%2C0%2C1900&cl_25973=&nfo_25959=V%2C0%2C1900&op_25959=0&txt_25959=&nfo_25946=V%2C4000%2C1900&op_25946=0&txt_25946=',
    1975: _START_BASE_URL + '&dt=2476&sc=25993%2C25962%2C25986%2C25942%2C25958%2C25973%2C25959%2C25946&nfo_25993=D%2C0%2C1900&op_25993=7&txt_25993=01%2F01%2F1975&txt_25993=&nfo_25962=V%2C0%2C1900&op_25962=0&txt_25962=&nfo_25986=V%2C0%2C1900&op_25986=0&txt_25986=&nfo_25942=V%2C0%2C1900&op_25942=0&txt_25942=&nfo_25958=V%2C100%2C1900&op_25958=0&txt_25958=&nfo_25973=V%2C0%2C1900&cl_25973=&nfo_25959=V%2C0%2C1900&op_25959=0&txt_25959=&nfo_25946=V%2C4000%2C1900&op_25946=0&txt_25946=',
    1976: _START_BASE_URL + '&dt=2082&sc=25993%2C25962%2C25986%2C25942%2C25958%2C25973%2C25959%2C25946&nfo_25993=D%2C0%2C1900&op_25993=7&txt_25993=01%2F01%2F1976&txt_25993=&nfo_25962=V%2C0%2C1900&op_25962=0&txt_25962=&nfo_25986=V%2C0%2C1900&op_25986=0&txt_25986=&nfo_25942=V%2C0%2C1900&op_25942=0&txt_25942=&nfo_25958=V%2C100%2C1900&op_25958=0&txt_25958=&nfo_25973=V%2C0%2C1900&cl_25973=&nfo_25959=V%2C0%2C1900&op_25959=0&txt_25959=&nfo_25946=V%2C4000%2C1900&op_25946=0&txt_25946=',
    1977: _START_BASE_URL + '&dt=2532&sc=27552%2C27521%2C27545%2C27501%2C27517%2C27532%2C27518%2C27505&nfo_27552=D%2C0%2C1900&op_27552=7&txt_27552=01%2F01%2F1977&txt_27552=&nfo_27521=V%2C0%2C1900&op_27521=0&txt_27521=&nfo_27545=V%2C0%2C1900&op_27545=0&txt_27545=&nfo_27501=V%2C0%2C1900&op_27501=0&txt_27501=&nfo_27517=V%2C100%2C1900&op_27517=0&txt_27517=&nfo_27532=V%2C0%2C1900&cl_27532=&nfo_27518=V%2C0%2C1900&op_27518=0&txt_27518=&nfo_27505=V%2C4000%2C1900&op_27505=0&txt_27505=',
    1978: _START_BASE_URL + '&dt=2694&sc=28761%2C28733%2C28754%2C28715%2C28729%2C28741%2C28730%2C28719&nfo_28761=D%2C0%2C1900&op_28761=7&txt_28761=01%2F01%2F1978&txt_28761=&nfo_28733=V%2C0%2C1900&op_28733=0&txt_28733=&nfo_28754=V%2C0%2C1900&op_28754=0&txt_28754=&nfo_28715=V%2C0%2C1900&op_28715=0&txt_28715=&nfo_28729=V%2C100%2C1900&op_28729=0&txt_28729=&nfo_28741=V%2C0%2C1900&cl_28741=&nfo_28730=V%2C0%2C1900&op_28730=0&txt_28730=&nfo_28719=V%2C4000%2C1900&op_28719=0&txt_28719=',
    1979: _START_BASE_URL + '&dt=2776&sc=29222%2C29194%2C29215%2C29176%2C29190%2C29202%2C29191%2C29180&nfo_29222=D%2C0%2C1900&op_29222=7&txt_29222=01%2F01%2F1979&txt_29222=&nfo_29194=V%2C0%2C1900&op_29194=0&txt_29194=&nfo_29215=V%2C0%2C1900&op_29215=0&txt_29215=&nfo_29176=V%2C0%2C1900&op_29176=0&txt_29176=&nfo_29190=V%2C100%2C1900&op_29190=0&txt_29190=&nfo_29202=V%2C0%2C1900&cl_29202=&nfo_29191=V%2C0%2C1900&op_29191=0&txt_29191=&nfo_29180=V%2C4000%2C1900&op_29180=0&txt_29180=',
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
        if res.status_code in (500, 503):
            time.sleep(5)
            res = sess.get(url)
        res.raise_for_status()
        page = res.content
        if not _META_REFRESH_PATTERN.search(page):
            break
        time.sleep(1)
    return page


def download_published_cables_overview_csv(directory, year, startpage=1, rpp=10, useragent=None):
    """\
    Creates CSV files of published cables for the provided year.

    This function stores one CSV file per page in the provided `directory`.

    :param str directory: Directory where to store the CSV files (one per page)
    :param int year: The year.
    :param int startpage: At which result page should the CSV generator start?
    :param int rpp: Result per page: 10 (default), 20, or 50
    :param str useragent: If ``None``, a default user agent string will be used.
    """
    def remove_multiple_ws(s):
        """\
        Folds two or more space characters into one.
        """
        return _MULTIPLE_WS_PATTERN.sub(' ', s)

    def next_page_url(html, base_url):
        """\
        Returns the link to the next page.
        """
        m = _NEXT_PAGE_PATTERN.search(html)
        if not m:
            return None
        return urljoin(base_url, m.group(1))

    def html_table_row_iter(page):
        """\
        Returns an iterator over the query result rows.
        """
        el = lxml.html.fromstring(page)
        for row in el.xpath('//table[@id="queryResults"]/tbody/tr'):
            children = row.xpath('td')[:-1]
            res_row = [children[0].xpath('a/@href')[0]]
            res_row.extend([child.text for child in children[1:]])
            yield res_row

    if rpp not in (10, 20, 50):
        raise ValueError('Expected 10, 20, or 50')

    sess = _make_session(useragent)
    get_html = partial(_get_page, sess)
    url = _YEAR_TO_STARTURL[year] + '&pg={0}&rpp={1}'.format(startpage, rpp)
    html = get_html(url)
    while html:
        page = _PG_PATTERN.search(url).group(1)
        f = open(os.path.join(directory, page) + '.csv', 'w', encoding='utf-8')
        writer = csv.writer(f)
        writerow = writer.writerow
        for doc_url, date, doc_no, film_no, sender, subject, tags, recipient in html_table_row_iter(html):
            subject = remove_multiple_ws(subject)
            doc_url = urljoin(url, doc_url)
            sender = sender or ''
            tags = tags or ''
            recipient = recipient or ''
            row = [doc_url, date, doc_no, film_no, sender, subject, tags, recipient]
            writerow(row)
        f.close()
        url = next_page_url(html, url)
        html = get_html(url) if url is not None else None


def merge_csv_files(directory, out):
    """\
    Merges the CSV files in the provided `directory` into one CSV file.

    :param str directory: Path where to find the CSV files
    :param str out: Resulting file name.
    """
    f = open(out, 'w', encoding='utf-8')
    writer = csv.writer(f)
    writerow = writer.writerow
    writerow(['URL', 'Draft Date', 'Document Number', 'Film Number', 'From', 'Subject', 'TAGS', 'To'])
    cnt = 0
    for fn in sorted(glob.glob(directory + '*.csv'), key=lambda fn: int(os.path.basename(fn).split('.')[0])):
        with open(fn, 'r', encoding='utf-8') as inputfile:
            reader = csv.reader(inputfile)
            for row in reader:
                cnt += 1
                writerow(row)
    f.close()
    return cnt


def download_cables(overview_csv, directory, offset=0, useragent=None):
    """\
    Downloads the cables given in `overview` into `directory`.

    :param str overview_csv: CSV generated by download_published_cables_overview_csv / merge_csv_files.
    :param str directory: The output directory.
    :param int offset: Offset in the CSV file (header row must not be taken into account).
    :param str useragent: The user agent string to use (``None`` indicates a
            default user agent string)
    """
    def month(dt):
        """\
        Returns the month from the provided `date`.
        """
        month_abr = _MONTH_PATTERN.search(dt).group(1).upper()
        return str(_MONTHS.index(month_abr) + 1).zfill(2)

    # Create directories for months
    for i in range(1, 13):
        month_dir = os.path.join(directory, str(i).zfill(2))
        if not os.path.exists(month_dir):
            os.makedirs(month_dir)
    index_file = open(os.path.join(directory, 'index.csv'), ('w' if not offset else 'a') , encoding='utf-8')
    writer = csv.writer(index_file)
    write_row = writer.writerow
    if not offset:
        write_row(['Document Number', 'Path', 'AAD filename', 'URL'])
    overview_file = open(overview_csv, 'r', encoding='utf-8')
    reader = csv.reader(overview_file)
    next(reader)  # Skip Header
    for i in range(offset):
        next(reader)
    sess = _make_session(useragent=useragent)
    for row in reader:
        doc_url = row[0]
        date = row[1]
        doc_no = row[2]
        local_path = month(date)
        res = sess.get(doc_url)
        if res.status_code in (500, 503):
            time.sleep(20)
            sess = _make_session(useragent=useragent)
            res = sess.get(doc_url)
        res.raise_for_status()
        aad_filename = _AAD_FILENAME_PATTERN.search(res.headers['Content-disposition']).group(1)
        local_filename = os.path.join(local_path, '{0}.pdf'.format(doc_no))
        filename = os.path.join(directory, local_filename)
        cnt = 0
        while os.path.isfile(filename):
            cnt += 1
            local_filename = os.path.join(local_path, '{0}-{1}.pdf'.format(doc_no, cnt))
            filename = os.path.join(directory, local_filename)
        with open(filename, 'wb') as f:
            f.write(res.content)
        write_row([doc_no, local_filename, aad_filename, doc_url])
    overview_file.close()
    index_file.close()
