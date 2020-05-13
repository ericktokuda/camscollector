#!/usr/bin/env python3
"""Windy
"""

import argparse
import time
import os
from os.path import join as pjoin
import inspect

import sys
import numpy as np
from itertools import product
import matplotlib; matplotlib.use('Agg')
import matplotlib.pyplot as plt
from multiprocessing import Pool
from datetime import datetime, timezone
import pytz
import requests
import json
import pandas as pd

apikey = os.getenv('WINDYKEY')
apitoken = os.getenv('WINDYTOKEN')
apitoken2 = os.getenv('WINDYTOKEN2')
if not apikey or not apitoken or not apitoken2:
    msg = 'Please check if WINDYKEY, WINDYTOKEN, WINDYTOKEN2 are set'
    raise Exception(msg)
MAXDELAY = 2

#############################################################
def info(*args):
    pref = datetime.now().strftime('[%y%m%d %H:%M:%S]')
    print(pref, *args, file=sys.stdout)

#############################################################
def list_archived_images(camid, tz, outpath):
    info(inspect.stack()[0][3] + '()')
    if os.path.exists(outpath): return pd.read_csv(outpath)

    root = 'https://node.windy.com/webcams/v2.0/archive/' + str(camid)
    payload = {'key': apikey}
    payload['token'] = apitoken
    payload['token2'] = apitoken2
    payload['uid'] = apitoken2
    r = requests.get(root, params=payload)
    time.sleep(np.random.rand() * MAXDELAY)
    info('GET: {}'.format(r.url))
    ret = r.json()

    frames = []
    for k, v in ret.items(): #day, month, year, lifetime
        for i, row in enumerate(v): # list of images
            aux = datetime.fromtimestamp(int(row['timestamp'] / 1000), tz=timezone.utc)
            datestr = aux.strftime("%Y%m%d_%H%M%S")
            filename = '{}-{}.jpg'.format(camid, datestr)
            frames.append([
                    camid,
                    k,
                    i,
                    row['url'],
                    datestr,
                    ])

    cols = 'camid,lapse,seqid,url,capturedon'.split(',')
    imgdf = pd.DataFrame(frames, columns=cols)
    imgdf.to_csv(outpath, index=False)
    return imgdf

#############################################################
def list_cameras(outpath, limit=5):
    info(inspect.stack()[0][3] + '()')

    if os.path.exists(outpath):
        info('Loading previously loaded cameras from ' + outpath)
        return pd.read_csv(outpath)

    payload = {'key': apikey}
    payload['show'] = 'webcams:location'

    rows = []
    for offset in range(0, limit, 50):
        root = 'https://api.windy.com/api/webcams/v2/list/orderby=popularity,desc/limit=50,{}'.format(offset)

        r = requests.get(root, params=payload)
        time.sleep(np.random.rand() * MAXDELAY)
        info('r.url:{}'.format(r.url))

        if r.status_code != 200:
            info('Execution returned code:{}'.format(r.status_code))
            break

        entries = r.json()['result']['webcams']
        if len(entries) == 0:
            info('No entries found in this range')
            break

        for i, entry in enumerate(entries):
            rows.append([
                    entry['id'],
                    entry['status'],
                    entry['location']['city'],
                    entry['location']['country_code'],
                    entry['location']['continent_code'],
                    entry['location']['latitude'],
                    entry['location']['longitude'],
                    entry['location']['timezone'],
                    ])

    cols = 'id,status,city,country_code,continent_code,'\
            'latitude,longitude,timezone'.split(',')
    df = pd.DataFrame(rows, columns=cols)
    df.to_csv(outpath, index=False)

    return df

##########################################################
def list_archived_images_all(camsdf, urldir):
    info(inspect.stack()[0][3] + '()')
    if not os.path.isdir(urldir): os.mkdir(urldir)

    for i, row in camsdf.iterrows():
        urlspath = pjoin(urldir, '{}.csv'.format(row.id))
        urlsdf = list_archived_images(row.id, row.timezone, urlspath)

##########################################################
def download_images(imgsdf, imgdir):
    for i, row in imgsdf.iterrows():
        imgpath = pjoin(imgdir, '{}-{}.jpg'.format(row.camid, row.capturedon))
        r = requests.get(row.url)
        info('GET: {}'.format(r.url))
        fh = open(imgpath, "wb")
        fh.write(r.content)
        fh.close()
        time.sleep(np.random.rand() * MAXDELAY)

##########################################################
def download_images_all(urldir, imgdir):
    info(inspect.stack()[0][3] + '()')

    if not os.path.isdir(urldir):
        info('urldir {} does not exist'.format(urldir))
        return
    elif not os.path.isdir(imgdir): os.mkdir(imgdir)

    files = sorted(os.listdir(urldir))
    for i, f in enumerate(files):
        imgsdf = pd.read_csv(pjoin(urldir, f))
        download_images(imgsdf, imgdir)

##########################################################
def main():
    info(inspect.stack()[0][3] + '()')
    t0 = time.time()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--limit', type=int, default=2 help='Max number of cameras')
    parser.add_argument('--outdir', type=str, default='/tmp/', help='outdir')
    args = parser.parse_args()

    if os.path.isdir(args.outdir):
        info('Folder {} already exists. Existing content will NOT be overwriten'. \
                format(args.outdir))
    else:
        os.mkdir(args.outdir)

    aux = list(product([args.outdir]))

    params = []
    for i, row in enumerate(aux):
        params.append(dict(outdir = row[0],))

    urldir = pjoin(args.outdir, 'url')
    imgdir = pjoin(args.outdir, 'img')
    camspath = pjoin(args.outdir, 'cams.csv')

    camsdf = list_cameras(camspath, limit=args.limit)
    list_archived_images_all(camsdf, urldir)
    download_images_all(urldir, imgdir)

    info('Elapsed time:{}'.format(time.time()-t0))

if __name__ == "__main__":
    main()


