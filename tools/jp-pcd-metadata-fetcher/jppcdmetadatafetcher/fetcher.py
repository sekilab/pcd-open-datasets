# -*- coding: utf-8 -*-
import json
import luigi
import lxml.html
import re
import os
import requests

from datetime import datetime
from urllib.parse import urljoin


def parse_japanera(datestr):
    offsets = {
        '昭和': 1926-1,
        '平成': 1989-1,
        '令和': 2019-1,
    }
    m = re.match(
        r'(\w{2})([0-9]+)年([0-9]+)月([0-9]+)日',
        datestr)
    if m is None:
        return datestr
    year, month, date = (
        offsets[m.group(1)]+int(m.group(2)),
        int(m.group(3)),
        int(m.group(4))
    )
    return datetime(year, month, date, 0, 0)


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


class BinaryDownloader(luigi.Task):
    filepath = luigi.Parameter()
    url = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(
            format=luigi.format.Nop, path=self.filepath)

    def run(self):
        r = requests.get(self.url, stream=True)
        if r.status_code == 200:
            with self.output().open('w') as f:
                # f.write(r.content)
                for chunk in r.iter_content(chunk_size=1024*10):
                    f.write(chunk)
        else:
            raise ConnectionError(r.status_code)


class ShizuokaPCDProductList(luigi.Task):
    url = luigi.Parameter(
        'https://pointcloud.pref.shizuoka.jp/lasmap/ankenmapsrc?request=MarkerSet&Xmax=139.6856689453125&Xmin=137.08053588867188&Ymax=36.097938036628065&Ymin=33.83962341851979')
    info_base_url = 'https://pointcloud.pref.shizuoka.jp/lasmap/ankendetail?ankenno={}'

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
                'name': {
                    'value': row[1],
                },
                'area': {
                    'type': 'geo:json',
                    'value': {
                        'type': 'Point',
                        'coordinates': [float(row[2]), float(row[3])]
                    }
                },
                'url': {
                    'value': 'project/{}.json'.format(row[0]),
                },
                'originalUrl': {
                    'value': self.info_base_url.format(row[0])
                },
            }
            records.append(record)

        with self.output().open('w') as f:
            json.dump(records, f, indent=2, ensure_ascii=False)


class ShizuokaPCDProductInfo(luigi.Task):
    info_base_url = 'https://pointcloud.pref.shizuoka.jp/lasmap/ankendetail?ankenno={}'
    geojson_base_url = 'https://pointcloud.pref.shizuoka.jp/lasmap/ankenmapsrc?request=GetGeoJson&KojiNo={}'
    product_id = luigi.Parameter()

    def requires(self):
        return [
            TextDownloader(
                os.path.join(
                    'tmp',
                    'shizuoka-pcd-product-info-{}.txt'.format(self.product_id)),
                self.info_base_url.format(self.product_id)),
            TextDownloader(
                os.path.join(
                    'tmp',
                    'shizuoka-pcd-product-geojson-{}.txt'.format(self.product_id)),
                self.geojson_base_url.format(self.product_id)),
        ]

    def output(self):
        return luigi.LocalTarget(
            os.path.join(
                'shizuokapcd', 'product', '{}.json'.format(self.product_id)))

    def run(self):
        info_raw, geojson_raw = [
            x.open('r').read()
            for x in self.input()
        ]
        # print(info_raw, geojson_raw)

        # parse info
        info_root = lxml.html.fromstring(info_raw)
        table = info_root.xpath('//table')[0]
        elems = list(filter(lambda x: type(x) is lxml.html.HtmlElement, table))
        columns = [x[0].text_content() for x in elems]
        values = [x[1] for x in elems]

        output_info = {}
        key_maps = {
            '工事番号': 'productNo',
            '案件名称': 'name',
            '受注業者': 'contractorName',
            '工期': 'constructionPeriod',
            '登録日付': 'registeredDate',
            '３Ｄデータ取得日': 'measuredDate',
            '出典明記方法': 'sourceStatement',
        }
        las_urls = []
        las_sizes = []
        for key, value in zip(columns, values):
            if key == 'ライセンス':
                product_license = value[0].get('href')
            elif key == '3Dデータ':
                for las_elem in value[0]:
                    # size
                    file_size = las_elem.text_content()\
                        .replace(') ', '').split('：')[-1]
                    las_sizes.append(file_size)
                    # url
                    las_url = urljoin(
                        'https://pointcloud.pref.shizuoka.jp/lasmap/',
                        las_elem[0].get('href'))
                    las_urls.append(las_url)
            else:
                output_info[key_maps[key]] = {
                    'value': str(value.text_content())
                }

        # reshape date
        for key in ['registeredDate', 'measuredDate']:
            value = output_info[key]['value']
            if type(value) is str and len(value) > 0:
                parsed_date = parse_japanera(value)
                output_info[key] = {
                    'value': parsed_date.strftime('%Y-%m-%d')
                }

        description = ', '.join([
            output_info[x]['value'] for x in [
                'name',
                'contractorName',
            ]
        ])

        geojson = json.loads(geojson_raw)

        output_info.update({
            'id': 'shizuoka-pcd-{}'.format(self.product_id),
            'type': 'PointCloudDatabase',
            'location': {
                'type': 'geo:json',
                'value': geojson['features'][0]['geometry']
            },
            'description': {
                'value': description,
            },
            'source': {
                'type': 'URL',
                'value': 'https://pointcloud.pref.shizuoka.jp',
            },
            'lasUrls': {
                'value': las_urls,
            },
            'lasSizes': {
                'value': las_sizes,
            },
            'license': {
                'type': 'URL',
                'value': product_license,
            },
        })

        with self.output().open('w') as f:
            json.dump(output_info, f, indent=2, ensure_ascii=False)


class ShizuokaPCDGenerateAllProductInfo(luigi.Task):
    def requires(self):
        return ShizuokaPCDProductList()

    def run(self):
        with self.input().open() as f:
            product_list = json.load(f)
        tasks = []
        for product in product_list:
            tasks.append(ShizuokaPCDProductInfo(product_id=product['id']))
        yield tasks


shizuoka_2019_pointcloud_xyz_list = [
    {'y': 906, 'x': 405, 'z': 10},
    {'y': 906, 'x': 406, 'z': 10},
    {'y': 907, 'x': 405, 'z': 10},
    {'y': 907, 'x': 406, 'z': 10},
]

shizuoka_2019_pointcloud_url_def = 'https://gic-shizuoka.s3-ap-northeast-1.amazonaws.com/2020/Vectortile/{data_type}00/{z}/{x}/{y}.pbf'

shizuoka_2019_pointcloud_type_list = ['ALB', 'LP', 'MMS']


class Shizuoka2019PointCloudFetchPBF(luigi.Task):
    x = luigi.IntParameter()
    y = luigi.IntParameter()
    z = luigi.IntParameter()
    data_type = luigi.Parameter()

    def get_url_params(self):
        params = {
            'x': self.x,
            'y': self.y,
            'z': self.z,
            'data_type': self.data_type,
        }
        return params

    def requires(self):
        params = self.get_url_params()
        url = shizuoka_2019_pointcloud_url_def.format(**params)
        filepath = os.path.join(
            'tmp',
            'shizuoka-2019-pointcloud-{data_type}-{z}-{x}-{y}.pbf'.format(**params))
        # TODO access denied
        return BinaryDownloader(filepath, url)

    def output(self):
        params = self.get_url_params()
        filepath = os.path.join(
            'tmp',
            'shizuoka-2019-pointcloud-{data_type}-{z}-{x}-{y}.txt'.format(**params))
        return luigi.LocalTarget(filepath)

    def run(self):
        print(self.output().path)
        raise NotImplementedError()


class Shizuoka2019PointCloudFetchAllPBFs(luigi.Task):
    def requires(self):
        for data_type in shizuoka_2019_pointcloud_type_list:
            for xyz in shizuoka_2019_pointcloud_xyz_list:
                params = dict(xyz, data_type=data_type)
        return Shizuoka2019PointCloudFetchPBF(**params)


def command():
    luigi.run()
