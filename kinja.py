#!/usr/bin/python3

import time
import urllib.request
from bs4 import BeautifulSoup as BS
from bs4 import Comment

import sqlite3

import plac


def work(con, sleep, author):
    page_ix = 0

    while True:
        url = 'https://kinja.com/{}?startIndex={}'.format(author, page_ix)
        page_ix += 20

        print(url)
        web = urllib.request.urlopen(url)

        soup = BS(web.read(), 'html.parser')

        articles = soup.find_all('article')
        if len(articles) == 0:
            break

        for article in articles:
            ts = article.find('time')['datetime']

            permalink = article.find('div', {'data-post-permalink': True})['data-post-permalink']

            title = article.find('h2').text.strip()

            ################################################################

            art_web = urllib.request.urlopen(permalink)
            soup = BS(art_web.read(), 'html.parser')
            #print(soup.prettify())
            body = soup.find('div', {'class': 'js_expandable-container'})

            for el in body(text=lambda text: isinstance(text, Comment)):
                el.extract()
            for el in body.find_all('figure'):
                el.decompose()
            for el in body.find_all('aside'):
                el.decompose()

            for ad in body.find_all('div', {'class': 'js_ad-mobile-dynamic'}):
                ad.decompose()
            for ad in body.find_all('div', {'class': 'js_movable_ad_slot'}):
                ad.decompose()
            for ad in body.find_all('div', {'class': 'ad-container'}):
                ad.decompose()
            for ad in body.find_all('div', {'class': 'instream-native-video'}):
                ad.decompose()
            for ad in body.find_all('div', {'class': 'video-embed'}):
                ad.decompose()
            for ad in body.find_all('div', {'class': 'embed-frame'}):
                ad.decompose()
            for ad in body.find_all('p', {'class': 'video-embed'}):
                ad.decompose()
            for ad in body.find_all('span', ['flex-video']):
                ad.decompose()

            for jslink in body.find_all('a', {'class': 'js_link'}):
                jslink.unwrap()

            #print(body)
            #print(body.text)
            print('>>> Date')
            print(ts)
            print('>>> Title')
            print(title)
            print('>>> URL')
            print(permalink)
            print('>>> Document')
            print(body)
            print()

            source = 'unknown-kinja'
            if 'lifehacker' in permalink:
                source = 'LifeHacker'
            elif 'themuse' in permalink:
                source = 'Muse'
            elif 'theslot' in permalink:
                source = 'Slot'
            elif 'splinternews' in permalink:
                source = 'SplinterNews'
            elif 'jezebel' in permalink:
                source = 'Jezebel'
            elif 'kinja' in permalink:
                source = 'Kinja'

            con.execute("insert into corpus (Source, Date, HasQuotes, Title, URL, Document) values (?, ?, ?, ?, ?, ?)", (source, ts, 1, title, permalink, str(body)))
            con.execute('commit;')

            time.sleep(sleep)

    pass



@plac.annotations(
    author=("Author name, e.g. johndoe", "positional", None, str),
    sleep=('Throttling sleep time, in seconds, between downloading articles from Kinja', 'option', 's', float),
    dbname=('Output Sqlite3 db name', 'option', 'w', str),
)
def main(author, sleep=1.0, dbname='kinja.sql'):
    con = sqlite3.connect(dbname)

    with con:
        con.execute("PRAGMA foreign_keys = 0")
        con.execute(
"""
CREATE TABLE IF NOT EXISTS "corpus" (
    `Source`    TEXT NOT NULL,
    `Date`  TEXT,
    `HasQuotes` INTEGER NOT NULL,
    `Title` TEXT,
    `Subtitle`  TEXT,
    `URL`   TEXT NOT NULL,
    `Document`  TEXT NOT NULL,
    `Tags`   TEXT
);
""")
        work(con, sleep, author)


if __name__ == '__main__':
    plac.call(main)
