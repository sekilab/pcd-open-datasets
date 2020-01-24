# -*- coding: utf-8 -*-
import json
import luigi
import os
import requests


class downloader(luigi.Task):
    filepath = luigi.Parameter()
    url = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(
            format=luigi.format.Nop, path=self.filepath)

    def run(self):
        r = requests.get(self.url)
        with self.output().open('wb') as f:
            f.write(r.content)


class ShizuokaPCDProductList(luigi.Task):
    url = luigi.Parameter(
        'https://pointcloud.pref.shizuoka.jp/lasmap/ankenmapsrc?request=MarkerSet&Xmax=139.6856689453125&Xmin=137.08053588867188&Ymax=36.097938036628065&Ymin=33.83962341851979')

    def requires(self):
        return downloader(os.path.join('tmp', 'shizuoka-pcd-product-list.txt'), self.url)

    def output(self):
        return luigi.LocalTarget(os.path.join('shizuokapcd', 'products.json'))

    def run(self):
        pass


def command():
    luigi.run()
