# -*- coding: utf-8 -*-
import json
import luigi
import os
import requests


class TextDownloader(luigi.Task):
    filepath = luigi.Parameter()
    url = luigi.Parameter()
    decode = luigi.Parameter(default='utf-8')

    def output(self):
        return luigi.LocalTarget(
            format=luigi.format.UTF8, path=self.filepath)

    def run(self):
        r = requests.get(self.url)
        with self.output().open('w') as f:
            f.write(r.content.decode(self.decode))


class ShizuokaPCDProductList(luigi.Task):
    url = luigi.Parameter(
        'https://pointcloud.pref.shizuoka.jp/lasmap/ankenmapsrc?request=MarkerSet&Xmax=139.6856689453125&Xmin=137.08053588867188&Ymax=36.097938036628065&Ymin=33.83962341851979')

    def requires(self):
        return TextDownloader(os.path.join('tmp', 'shizuoka-pcd-product-list.txt'), self.url)

    def output(self):
        return luigi.LocalTarget(os.path.join('shizuokapcd', 'products.json'))

    def run(self):
        with self.input().open() as f:
            rawdata = f.read()
        records = []
        for raw in rawdata.split('?'):
            row = raw.split(':')
            if len(row) < 4:
                continue
            record = {
                'id': row[0],
                'name': row[1],
                'lon': float(row[2]),
                'lat': float(row[3]),
            }
            records.append(record)

        with self.output().open('w') as f:
            json.dump(records, f, indent=2, ensure_ascii=False)


def command():
    luigi.run()
