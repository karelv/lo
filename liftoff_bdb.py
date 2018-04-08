from bigchaindb_driver import BigchainDB
from bigchaindb_driver.crypto import generate_keypair
import click
import yaml
import json
from pprint import pprint
import calendar
import time

bdb_cfg = {}
bdb = None

def get_db_data(obj, obj_id, data):
  global bdb_cfg
  now_epoch = calendar.timegm(time.gmtime())
  db_data = {'data': {
      'created_at': now_epoch,
      'index': '__liftoff__{app_key} {obj} {obj_id} '.format (app_key=bdb_cfg['headers']['app_key'], obj=obj, obj_id=obj_id),
      'object-type': obj,
      'object-id': obj_id,
      obj: data,
    }
  }
  return db_data

def do_create (obj, obj_id, data, metadata):
  global bdb_cfg, bdb
  db_data= get_db_data(obj, obj_id, data)

  db_metadata = metadata
  if metadata is not None:
    db_metadata['created_at'] = db_data['data']['created_at']

  tx = bdb.transactions.prepare(operation='CREATE', signers=bdb_cfg['user']['public_key'],
    asset=db_data, metadata=db_metadata)

  txid = tx['id']

  signed_tx = bdb.transactions.fulfill( tx, private_keys=bdb_cfg['user']['private_key'])

  if (signed_tx != bdb.transactions.send(signed_tx)):
    return False
  return txid


def do_get_search_query(obj, obj_id):
  global bdb_cfg
  search_query = '"__liftoff__{}'.format(bdb_cfg['headers']['app_key'])
  if obj is not None:
    search_query += ' {}'.format(obj)
    if obj_id is not None:
      search_query += ' {}'.format(obj_id)
  search_query += ' "'
  return search_query


def do_read (obj, obj_id):
  global bdb

  search_query = do_get_search_query(obj, obj_id)

  assets = bdb.assets.get(search=search_query)

  for asset in assets:
    asset['details'] = bdb.transactions.get(asset_id=asset['id'])
  return assets


def do_read_by_query (query):
  global bdb
  assets = bdb.assets.get(search=query)

  for asset in assets:
    asset['details'] = bdb.transactions.get(asset_id=asset['id'])
  return assets


def do_print_assets(assets):
  for asset in assets:
    asset_details = asset['details']
    print ("asset")
    print ("=====")
    print ("")
    first_time = True
    for ad in asset_details:
      pprint (">>>")
      if first_time:
        print ('asset-id: {}'.format (ad['id']))
        first_time = False
      else:
        print ('transaction-id: {}'.format (ad['id']))
      if 'data' in ad['asset']:
        print ("data:")
        pprint (ad['asset']['data'])
      if 'metadata' in ad:
        print ("metadata:")
        pprint (ad['metadata'])
      pprint ("<<<")


def get_transfer_input (transfer_asset):
  asset_details = bdb.transactions.get(asset_id=transfer_asset['id'])

  output_index = 0
  output = asset_details[0]['outputs'][output_index]
  transfer_input = {
    'fulfillment': output['condition']['details'],
    'fulfills': {
      'output_index': output_index,
      'transaction_id': asset_details[-1]['id'],
    },
    'owners_before': output['public_keys'],
  }
  return transfer_input


def do_append(asset_id, metadata):
  global bdb_cfg, bdb

  now_epoch = calendar.timegm(time.gmtime())
  db_metadata = metadata
  db_metadata['appended_at'] = now_epoch

  transfer_asset = { 'id': asset_id }

  transfer_input = get_transfer_input (transfer_asset)

  prepared_transfer_tx = bdb.transactions.prepare(operation='TRANSFER', asset=transfer_asset,
    inputs=transfer_input, recipients=bdb_cfg['user']['public_key'], metadata=db_metadata)

  fulfilled_transfer_tx = bdb.transactions.fulfill(prepared_transfer_tx,
    private_keys=bdb_cfg['user']['private_key'])

  sent_transfer_tx = bdb.transactions.send(fulfilled_transfer_tx)
  if (sent_transfer_tx != fulfilled_transfer_tx):
    return False

  return sent_transfer_tx['id']


def do_burn(asset_id):
  global bdb_cfg, bdb

  now_epoch = calendar.timegm(time.gmtime())
  db_metadata = { 'burned_at': now_epoch }

  transfer_asset = { 'id': asset_id }

  transfer_input = get_transfer_input (transfer_asset)

  lost_user = generate_keypair ()

  prepared_transfer_tx = bdb.transactions.prepare(operation='TRANSFER',  asset=transfer_asset,
    inputs=transfer_input, recipients=lost_user.public_key, metadata=db_metadata)

  fulfilled_transfer_tx = bdb.transactions.fulfill(prepared_transfer_tx,  private_keys=bdb_cfg['user']['private_key'])

  sent_transfer_tx = bdb.transactions.send(fulfilled_transfer_tx)
  if (sent_transfer_tx != fulfilled_transfer_tx):
    return False

  return sent_transfer_tx['id']


