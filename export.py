import json
import os

import pymongo
import sqlalchemy as db
import pandas as pd


def export_graphics_cards():

    project_path = os.environ['HOME'] + '/PROJECT/DEC_PJ5_GCP/'

    # Connect to MySQL
    engine = db.create_engine('mysql+pymysql://root:123456@localhost:3306/graphics_cards')
    conn = engine.connect()

    # Export data from table `products`
    data = pd.read_sql(db.text('SELECT * FROM products;'), conn)
    data.to_csv(project_path + 'export/graphics_cards.csv', index=False)

    conn.close()


def export_tiki_products():

    project_path = os.environ['HOME'] + '/PROJECT/DEC_PJ5_GCP/'

    # File output
    out_put_product = open(project_path + 'export/product.json', 'w')

    # Connect to MongoDB
    mongo_server = pymongo.MongoClient('mongodb://localhost:27017/')
    mongo_db = mongo_server['TIKI_NEW']
    mongo_coll_product = mongo_db["product"]

    products = mongo_coll_product.find({})
    for prod in products:
        prod['_id'] = str(prod['_id'])  # Change ObjectID of Mongo to String
        prod['crawled_time'] = str(prod['crawled_time'])  # Change Date to String
        out_put_product.write(json.dumps(prod) + "\n")

    out_put_product.close()


def copy_to_gcs(bucket_name):

    project_path = os.environ['HOME'] + '/PROJECT/DEC_PJ5_GCP/'
    cmd = 'gsutil -o GSUtil:parallel_composite_upload_threshold=150M -m cp -r %s gs://%s' % (project_path, bucket_name)
