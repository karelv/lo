from bigchaindb_driver import BigchainDB
from bigchaindb_driver.crypto import generate_keypair
import click
import yaml
import json
from pprint import pprint
import calendar
import time

bdb_cfg = {}
config_file = None
bdb = None

def load_cfg (cfg_file):
  global bdb_cfg, config_file
  config_file = cfg_file
  with open('liftoff.yml', 'r') as f:
    bdb_cfg = yaml.load(f)

@click.group()
@click.option('--config', help='configuration file.', default='liftoff.yml')
def cli(config):
  """ LIFTOFF interface to blockchain. """
  global bdb_cfg, config_file, bdb
  load_cfg (config)

  bdb = BigchainDB(
    bdb_cfg['bigchaindb']['url'],
    headers=bdb_cfg['headers'])

@cli.command()
@click.option('--name', help='name of the user.')
@click.option('--private-key', is_flag=True, help='show also private key.')
def user(name, private_key):
  """ Create/show user and it's keypairs """
  global bdb_cfg, config_file

  if name is not None:
    new_user = generate_keypair()

    bdb_cfg['user'] = {}
    bdb_cfg['user']['name'] = name
    bdb_cfg['user']['public_key'] = new_user.public_key
    bdb_cfg['user']['private_key'] = new_user.private_key
    yml_str = yaml.dump(bdb_cfg, width=80, indent=4, default_flow_style=False)
    with open (config_file, 'w') as f:
      f.write (yml_str)
    print ("configuration file '{}' updated.".format(config_file))
  else:
    show_keys = ['name', 'public_key']
    if private_key:
      show_keys.append('private_key')
    for key in show_keys:
      print ("{:>15s}: {}".format(key, bdb_cfg['user'][key]))

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

@cli.command()
@click.option('--obj', required=True, help='the object type name to create.')
@click.option('--obj-id', required=True, help='the object ID to create.')
def create(obj, obj_id):
  """ Create (CRAB) data in the bigchaindb. """
  data = {
    'serial_number': 'QWERTY',
    'manufacturer': 'catan',
    'people_capacity': 4,
    'package_capacity': 2,
    'batery_capacity': 100,
  }

  metadata = {
    'odo': 10,
    'lat': 53,
    'lon': 6.6,
    'batery_charge_level': 50,
    'state': 'idle',
  }
  r = do_create (obj, obj_id, data, metadata)
  if r == False:
    print ("ERROR: Failed to create asset...")
  else:
    print ("Success: created asset '{}'".format (r))

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

@cli.command()
@click.option('--obj', help='the object type name to read.')
@click.option('--obj-id', help='the object ID to read.')
def read(obj, obj_id):
  """ Read (CRAB) data from the bigchaindb. """

  assets = do_read (obj, obj_id)

  do_print_assets(assets)

@cli.command()
@click.option('--query', help='the query to execute.')
def query(query):
  """ Read by query (CRAB) data from the bigchaindb. """

  assets = do_read_by_query (query)

  do_print_assets(assets)

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

@cli.command()
@click.option('--asset-id', help='the asset-id of the object to append metadata to.')
@click.option('--odo', help='append odo to metadata.')
def append(asset_id, odo):
  """ Append (CRAB) metadata to the bigchaindb. """

  metadata = {'odo': odo}
  r = do_append (asset_id, metadata)
  if r == False:
    print ("ERROR: Failed to append asset...")
  else:
    print ("Success: append asset with transaction-id: '{}'".format (r))

@cli.command()
@click.option('--asset-id', help='the asset-id of the object to append metadata to.')
def burn(asset_id):
  """ Burn (CRAB) metadata to the bigchaindb. """

  r = do_burn (asset_id)
  if r == False:
    print ("ERROR: Failed to append asset...")
  else:
    print ("Success: burn asset with transaction-id: '{}'".format (r))

@cli.command()
@click.option('--name', required=True, help='the name of the playground to be created.')
def playground(name):
  """ Create the playground. """

  data = {
    'name': name,
  }

  r = do_create ('playground', name, data, None)
  if r == False:
    print ("ERROR: Failed to create asset...")
  else:
    print ("Success: created asset '{}'".format (r))

@cli.command()
@click.option('--playground', required=True, help='the playground forwhich the simulation is wanted.')
@click.option('--simulation-file', required=True, help='the json file with simulation information.')
def upload_simulation (playground, simulation_file):
  """ Upload simulation to the playground. """
  assets = do_read ('playground', playground)
  if len (assets) != 1:
    print ("ERROR: playground not uniquely available in bigchaindb.")
    return

  playground = assets[0]

  metadata = json.load(open(simulation_file, 'r'))

  r = do_append (playground['id'], metadata)
  if r == False:
    print ("ERROR: Failed to append asset...")
  else:
    print ("Success: append asset with transaction-id: '{}'".format (r))

@cli.command()
@click.option('--playground', required=True, help='the playground forwhich the simulation is wanted.')
@click.option('--simulation-file', required=True, help='the json file with simulation information.')
def download_simulation (playground, simulation_file):
  """ Upload simulation to the playground. """
  assets = do_read ('playground', playground)
  if len (assets) != 1:
    print ("ERROR: playground not uniquely available in bigchaindb.")
    return

  playground = assets[0]

  simulations = []

  for transaction in playground['details']:
    metadata = transaction['metadata']
    if metadata is not None:
      simulations.append(metadata)

  with open (simulation_file, 'w') as out:
    out.write (json.dumps(simulations))

if __name__ == "__main__":
  cli()
