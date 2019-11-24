#!/usr/bin/python3

import time
import json
import urllib.request
from bs4 import BeautifulSoup as BS
from bs4 import Comment

import sqlite3

import plac


def work(con, sleep, author):
    page_ix = 0
    page_sz = 12

    while True:
        url = 'https://cms.prod.nypr.digital/api/v2/pages/?type=news.ArticlePage&fields=publication_date,url,title,body,tags&order=-publication_date&show_on_index_listing=true&author_slug={}&limit={}&offset={}'.format(author, page_sz, page_ix)
        page_ix += page_sz

        print(url)
        web = urllib.request.urlopen(url)

        js = json.loads(web.read())

        articles = js['items']
        if len(articles) == 0:
            break

        for article in articles:
            ts = article['publication_date']
            permalink = article['url']
            title = article['title']
            #tags = ' '.join(t['slug'] for t in article['tags'])
            tags = ','.join(t['name'] for t in article['tags'])

            body = '\n'.join(p['value']['code'] for p in article['body'])
            body = body.replace('<href', '<a href')
            body = body.replace('</href', '</a href')
            body = body.replace('<http:', '<a')
            body = body.replace('</http:', '</a')

            body = BS(body, 'html.parser')

            for el in body('a'):
                el.unwrap()
            for el in body('blockquote.given'):
                el.unwrap()
            for el in body('enter'):
                el.unwrap()
            for el in body('center'):
                el.unwrap()
            for el in body('cente'):
                el.unwrap()
            for el in body('em(nell'):
                el.unwrap()
            for el in body('strong<'):
                el.unwrap()
            for el in body('em<[citation'):
                el.unwrap()

            for el in body('iframe'):
                el.decompose()
            for el in body('img'):
                el.decompose()
            for el in body('script'):
                el.decompose()
            for el in body('form'):
                el.decompose()
            for el in body('object'):
                el.decompose()
            for el in body('href'):
                el.decompose()
            for el in body('blockquote', {'class': 'twitter-tweet'}):
                el.decompose()
            for el in body('blockquote', {'class': 'reddit-card'}):
                el.decompose()
            for el in body('blockquote', {'class': '“twitter-tweet”'}):
                el.decompose()
            for el in body('blockquote', {'class': 'twitter-video'}):
                el.decompose()
            for el in body('blockquote', {'class': 'imgur-embed-pub'}):
                el.decompose()
            for el in body('blockquote', {'class': 'instagram-media'}):
                el.decompose()
            for el in body('span', {'class': 'mt-enclosure-image'}):
                el.decompose()
            for el in body('span', {'class': 'photo_caption'}):
                el.decompose()
            for el in body('div', {'id': 'fb-root'}):
                el.decompose()
            for el in body('div', {'class': 'fb-post'}):
                el.decompose()
            for el in body('div', {'class': 'fb-video'}):
                el.decompose()
            for el in body('div', {'id': 'ooyalaplayer'}):
                el.decompose()
            for el in body('div', {'class': 'image-none'}):
                el.decompose()
            for el in body('div', {'class': 'image-right'}):
                el.decompose()
            for el in body('div', {'class': 'image-left'}):
                el.decompose()
            for el in body('div', {'class': 'image-nne'}):
                el.decompose()
            for el in body('div', {'class': 'tableauPlaceholder'}):
                el.decompose()
            for el in body(text=lambda text: isinstance(text, Comment)):
                el.extract()


            print('>>> Date')
            print(ts)
            print('>>> Title')
            print(title)
            print('>>> Tags')
            print(tags)
            print('>>> URL')
            print(permalink)
            print('>>> Document')
            print(body)
            print()

            source = 'Gothamist'

            con.execute("insert into corpus (Source, Date, HasQuotes, Title, URL, Document, Tags) values (?, ?, ?, ?, ?, ?, ?)", (source, ts, 1, title, permalink, str(body), tags))
            con.execute('commit;')

        time.sleep(sleep)

    pass



@plac.annotations(
    author=("Author name, e.g. john-doe", "positional", None, str),
    sleep=('Throttling sleep time, in seconds, between downloading batch of articles from Gothamist', 'option', 's', float),
    dbname=('Output Sqlite3 db name', 'option', 'w', str),
)
def main(author, sleep=3.0, dbname='gothamist.sql'):
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
