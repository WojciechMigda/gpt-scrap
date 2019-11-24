#!/usr/bin/python3

import time
import datetime
import json
import urllib.request
from bs4 import BeautifulSoup as BS
from bs4 import Comment

import sqlite3

import plac


def do_nodes(con, sleep, nodes):
    print(len(nodes))
    for node in nodes:
        title = node['title']
        ts = datetime.datetime.fromtimestamp(int(node['post']['publishedAt']) / 1000.).isoformat()
        permalink = node['post']['url']

        web = urllib.request.urlopen(permalink)
        soup = BS(web.read(), 'html.parser')
        article = soup.find('article')
        #print(article)
        body = article.find('div', {'class': 'km'})

        for el in body('a'):
            el.unwrap()

        for el in body('iframe'):
            el.decompose()
        for el in body('img'):
            el.decompose()
        for el in body('figure'):
            el.decompose()
        for el in body('script'):
            el.decompose()
        for el in body('noscript'):
            el.decompose()
        for el in body('form'):
            el.decompose()
        for el in body('object'):
            el.decompose()
        for el in body('div', {'class': 'jy'}):
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

        print('>>> Date')
        print(ts)
        print('>>> Title')
        print(title)
        print('>>> URL')
        print(permalink)
        print('>>> Document')
        print(body)
        print()

        source = 'Bustle'

        con.execute("insert into corpus (Source, Date, HasQuotes, Title, URL, Document) values (?, ?, ?, ?, ?, ?)", (source, ts, 1, title, permalink, str(body)))
        con.execute('commit;')

        time.sleep(sleep)



def fetch_clip_cxn(page_info):
    after = page_info['endCursor']
    print('Fetch {}'.format(after))
    url = 'https://graph.bustle.com/?query=query(%24zoneId%3AID!%2C%24site%3ASiteName!%2C%24first%3AInt%20%3D%2020%2C%24after%3AString%20%3D%20%220%22%2C%24before%3AString%20%3D%20%220%22)%7Bsite(name%3A%24site)%7Bzone(id%3A%24zoneId)%7B...on%20ListZone%7BclipConnection(first%3A%24first%2Cafter%3A%24after%2Cbefore%3A%24before)%7Bnodes%7B...AllClipsFragment%7DpageInfo%7BhasNextPage%20startCursor%20endCursor%7D%7D%7D%7D%7D%7Dfragment%20AllClipsFragment%20on%20Clip%7B...PostClipFragment...PageClipFragment...MobiledocClipFragment...ImageClipFragment...OembedClipFragment...ProductClipFragment...AdClipFragment...VideoClipFragment%7Dfragment%20PostClipFragment%20on%20PostClip%7Btype%3A__typename%20id%20title%20summary%20primaryMedia%7B...on%20Image%7Burl%20height%20width%20description%7D%7Dpost%7Btype%3A__typename%20url%20path%20publishedAt%20sponsored%20vertical%7Bname%7Dsite%7Bname%20title%7DauthorConnection%7Bnodes%7Bname%20path%20primaryMedia%7B...on%20Image%7Burl%7D%7D%7D%7D...on%20FlowchartPost%7Btitle%20svgUrl%20backgroundColor%20controlsColor%20sponsorImageUrl%7D...on%20VideoPost%7BvideoKey%7D%7D%7Dfragment%20PageClipFragment%20on%20PageClip%7Btype%3A__typename%20id%20title%20pageDescription%3Adescription%20primaryMedia%7B...on%20Image%7Burl%20width%20height%20description%7D%7Dpage%7Bpath%20url%20description%20sponsored%20publishedAt%20site%7Bname%7DzoneConnection%7Bnodes%7BclipConnection(first%3A1)%7Bnodes%7B...PostClipFragment%7D%7D%7D%7D%7D%7Dfragment%20MobiledocClipFragment%20on%20MobiledocClip%7Btype%3A__typename%20id%20mobiledoc%20linkUrl%20button%20buttonText%7Dfragment%20ImageClipFragment%20on%20ImageClip%7Btype%3A__typename%20id%20linkUrl%20button%20buttonText%20buttonPosition%20panoramic%20caption%20image%7Burl%20attribution%20attributionUrl%20description%20width%20height%7D%7Dfragment%20OembedClipFragment%20on%20OembedClip%7Btype%3A__typename%20id%20url%20title%20primaryMedia%7B...on%20Image%7Burl%7D%7Doembed%7Burl%20providerName%7D%7Dfragment%20ProductClipFragment%20on%20ProductClip%7Btype%3A__typename%20id%20description%20linkUrl%20button%20buttonText%20product%7Bid%20name%20source%20description%20linkUrl%20primaryMedia%7B...on%20Image%7Burl%20width%20height%7D%7Dprice%7Bamount%20currency%7DdiscountPrice%7Bamount%20currency%7D%7D%7Dfragment%20AdClipFragment%20on%20AdClip%7Btype%3A__typename%20id%7Dfragment%20VideoClipFragment%20on%20VideoClip%7Btype%3A__typename%20id%20autoplay%20controls%20loop%20muted%20poster%3Aimage%7Burl%7Dvideo%7Bstate%20max%3Astream(quality%3Ahigh)%7Burl%20width%20height%20duration%7Dlow%3Astream(quality%3Amedium)%7Burl%20width%20height%20duration%7D%7D%7D&variables=%7B%22zoneId%22%3A%2218771971%22%2C%22after%22%3A%22{}%22%2C%22site%22%3A%22BUSTLE%22%7D'.format(after)
    web = urllib.request.urlopen(url)
    js = json.loads(web.read())
    return js['data']['site']['zone']['clipConnection']


def work(con, sleep, author):
    boot_url = 'https://www.bustle.com/profile/{}'.format(author)
    boot_web = urllib.request.urlopen(boot_url)

    boot = BS(boot_web.read(), 'html.parser')

    initial_state = [el.text for el in boot('script') if el.text.startswith('__INITIAL_STATE__=')]
    if not initial_state:
        print('Cannot find <script> tag with __INITIAL_STATE__ . Aborting')
        return

    js = json.loads(initial_state[0].split('__INITIAL_STATE__=')[1])
    znodes = js['zoneConnection']['nodes']
    for znode in znodes:
        clip_cxn = znode['clipConnection']
        nodes = clip_cxn['nodes']
        page_info = clip_cxn['pageInfo']

        while True:
            do_nodes(con, sleep, nodes)

            if not page_info['hasNextPage']:
                break

            clip_cxn = fetch_clip_cxn(page_info)
            nodes = clip_cxn['nodes']
            page_info = clip_cxn['pageInfo']

    pass


@plac.annotations(
    author=("Author name, e.g. john-doe-1234567", "positional", None, str),
    sleep=('Throttling sleep time, in seconds, between downloading articles from Bustle', 'option', 's', float),
    dbname=('Output Sqlite3 db name', 'option', 'w', str),
)
def main(author, sleep=1.0, dbname='bustle.sql'):
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
