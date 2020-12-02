import os
import time
import datetime
import requests
import bs4
from urllib.parse import urljoin
import pymysql
from pymysql import Error
import dotenv

pymysql.install_as_MySQLdb()


dotenv.load_dotenv('.env')

def connect():
    """ Connect to MySQL database """
    try:
        conn = pymysql.connect('localhost', os.getenv('user'), os.getenv('password'), 'Classificator')
        print('Successfull connection to database')
        return conn
    except Error:
        print('Connection Failed')

start_url = 'https://www.auchan.ru/'

class Parser:

    _headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:83.0) Gecko/20100101 Firefox/83.0'
    }
    _table = 'auchan_catalogue'

    db_format = {
        'retailer': 'Auchan',
        'category_name': '',
        'category_url': '',
        'subcategory_name': '',
        'subcategory_url': '',
        'product_name': '',
        'product_url': '',
        'submission_date': datetime,
    }

    def __init__(self, start_url):
        self.start_url = start_url
        self.category_template = {
            'category_url': lambda soup: urljoin(self.start_url, soup.get('href')),
            'category_name': lambda soup: soup.find('span', attrs={'class': 'css-zocp36'}).text,
        }
        self.subcategory_template = {
            'subcategory_url': lambda soup: urljoin(self.start_url, soup.get('href')),
            'subcategory_name': lambda soup: soup.text,
        }
        self.products_template = {
            'product_url': lambda soup: urljoin(self.start_url, soup.find('a', attrs={'class': 'productCardPictureLink css-3d15b0'}).get('href')),
            'product_name': lambda soup: soup.find('a', attrs={'class': 'linkToPDP css-1kl2eos'}).text,
        }


    @staticmethod
    def _get(*args, **kwargs) -> requests.Response:
        while True:
            try:
                response = requests.get(*args, **kwargs)
                if response.status_code != 200:
                    raise Exception
                return response
            except Exception:
                time.sleep(0.25)

    def soup(self, url) -> bs4.BeautifulSoup:
        response = self._get(url, headers=self._headers)
        return bs4.BeautifulSoup(response.text, 'lxml')

    def get_categories(self, soup):
        categories = soup.find_all('a', attrs={'class': 'youMayNeedCategoryItem css-1poaokp'})

        for category in categories:
            cat_data = self.get_cat_data(category)
            yield cat_data

        return categories

    def get_cat_data(self, categories):
        result = {}
        for key, value in self.category_template.items():
            try:
                result[key] = value(categories)
            except Exception as e:
                continue
        return result

    def get_subcategories(self, url):
        soup = self.soup(url)
        subcategories = soup.find_all('a', attrs={'class': 'linkToSubCategory css-pmm0xw'})

        for subcategory in subcategories:
            subcat_data = self.get_subcat_data(subcategory)
            yield subcat_data

        return subcategories

    def get_subcat_data(self, subcategories):
        result = {}
        for key, value in self.subcategory_template.items():
            try:
                result[key] = value(subcategories)
            except Exception as e:
                continue
        return result

    def get_product(self, url):
        soup = self.soup(url)
        products = soup.find_all('article', attrs={'class': 'productCard css-13rrxoq'})
        for product in products:
            pr_data =  self.get_pr_data(product)
            yield pr_data

        return products

    def get_pr_data(self, products):
        result = {}
        for key, value in self.products_template.items():
            try:
                result[key] = value(products)
            except Exception as e:
                continue
        return result


    def run(self):
        soup = self.soup(self.start_url)
        for category in self.get_categories(soup):
            self.db_format['category_name'] = category['category_name']
            self.db_format['category_url'] = category['category_url']
            url = category['category_url']
            for subcategory in self.get_subcategories(url):
                self.db_format['subcategory_name'] = subcategory['subcategory_name']
                self.db_format['subcategory_url'] = subcategory['subcategory_url']
                url = subcategory['subcategory_url']
                for product in self.get_product(url):
                    self.db_format['product_name'] = product['product_name']
                    self.db_format['product_url'] = product['product_url']
                    self.db_format['submission_date'] = datetime.datetime.now()
                    self.save(self.db_format)


    def save(self, obj):
        conn = connect()
        curr = conn.cursor()

        placeholder = ", ".join(["%s"] * len(obj))
        stmt = "insert into `{table}` ({columns}) values ({values});".format(table=self._table,
                                                                             columns=",".join(obj.keys()),
                                                                             values=placeholder)
        curr.execute(stmt, list(obj.values()))
        conn.commit()
        conn.close()


if __name__ == '__main__':
    parser = Parser(start_url)
    parser.run()


