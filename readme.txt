Bigchaindb: https://testnet.bigchaindb.com/login
Username: liftoff
Email: karel.vanroye@gmail.com
Password: 2Play@Catan
Docs: https://docs.bigchaindb.com/projects/py-driver
Blog: https://blog.bigchaindb.com/introducing-queryable-assets-in-bigchaindb-v-1-0-adbe1b86e622
CRAB:(javascript) https://tutorials.bigchaindb.com/crab/
Get started: https://www.bigchaindb.com/getstarted/


“Asset” has “data“ and “metadata”.
“Data” is immutable, non-appendable, and is available in the first transaction.
“Data” can be queried with text field
Query with exclude text field possible
Query with “Phrase” or exact match possible
Query with “oring” words is possible by a ‘space’.
Query with “anding” words is NOT possible!
“Metadata” is immutable, appendable, and can thus be updated very TRANSFER transaction.
Query is not possible on metadata.
“Queries” search the entire database
No concept of tables
Add ‘app_key’ to the field ⇒ our app can search our data.
No AND query with text field possible
Add ‘index’ parameter with our app-key, and the most often needed queries ⇒ we can search with “exact” match.
Results of queries can be limited (in amount), note: meta data is not ‘merged’.




asset-id: dc5c59ff2bfbda5cc4064543db579f4f10712201880234b1f9d1eb9b0677b967
data:
{'name': 'tu_for_append',
 'tu': {'batery_capacity': 100,
        'manufacturer': 'catan',
        'package_capacity': 2,
        'people_capacity': 4,
        'serial_number': 'tu_for_append'},
 'us': '__liftoff__323e58f2f88da58e4dba8f35c4699823'}
metadata:
{'batery_charge_level': 50, 'lat': 53, 'lon': 6.6, 'odo': 10, 'state': 'idle'}
'<<<'
⇒ here meta data is appended, only the metadata ‘odo’ was supplied
'>>>'
metadata:
{'odo': 111}
'<<<'






kruistabel@kruistabel:~/bigchaindb$ python3 liftoff.py create --help
Usage: liftoff.py create [OPTIONS]

  Create (CRAB) data in the bigchaindb.

Options:
  --obj TEXT     the object type name to create.  [required]
  --obj-id TEXT  the object ID to create.  [required]
  --help         Show this message and exit.
kruistabel@kruistabel:~/bigchaindb$ python3 liftoff.py create --obj tu --obj-id tu_liftoff
created asset 681172b5702bc401b0e42d086d632f37cc9b266cdce95e9d1b8d48f84beb2563
kruistabel@kruistabel:~/bigchaindb$ python3 liftoff.py read --query tu_liftoff
results for query: 'tu_liftoff'
-------------------------------
asset
=====

'>>>'
asset-id: 681172b5702bc401b0e42d086d632f37cc9b266cdce95e9d1b8d48f84beb2563
data:
{'created_at': 1523105166,
 'index': '__liftoff__323e58f2f88da58e4dba8f35c4699823__tu__tu_liftoff',
 'object-id': 'tu_liftoff',
 'object-type': 'tu',
 'tu': {'batery_capacity': 100,
        'manufacturer': 'catan',
        'package_capacity': 2,
        'people_capacity': 4,
        'serial_number': 'QWERTY'}}
metadata:
{'batery_charge_level': 50, 'lat': 53, 'lon': 6.6, 'odo': 10, 'state': 'idle'}
'<<<'
kruistabel@kruistabel:~/bigchaindb$ python3 liftoff.py append --help
Usage: liftoff.py append [OPTIONS]

  Append/Burn (CRAB) metadata to the bigchaindb.

Options:
  --asset-id TEXT  the asset-id of the object to append metadata to.
                   [required]
  --odo TEXT       append odo to metadata.
  --burn           burn the entry in the database.
  --help           Show this message and exit.
kruistabel@kruistabel:~/bigchaindb$ python3 liftoff.py append --asset-id 681172b5702bc401b0e42d086d632f37cc9b266cdce95e9d1b8d48f84beb2563 --odo 0
Append successful to asset-id '681172b5702bc401b0e42d086d632f37cc9b266cdce95e9d1b8d48f84beb2563'
kruistabel@kruistabel:~/bigchaindb$ python3 liftoff.py read --query tu_liftoff
results for query: 'tu_liftoff'
-------------------------------
asset
=====

'>>>'
asset-id: 681172b5702bc401b0e42d086d632f37cc9b266cdce95e9d1b8d48f84beb2563
data:
{'created_at': 1523105166,
 'index': '__liftoff__323e58f2f88da58e4dba8f35c4699823__tu__tu_liftoff',
 'object-id': 'tu_liftoff',
 'object-type': 'tu',
 'tu': {'batery_capacity': 100,
        'manufacturer': 'catan',
        'package_capacity': 2,
        'people_capacity': 4,
        'serial_number': 'QWERTY'}}
metadata:
{'batery_charge_level': 50, 'lat': 53, 'lon': 6.6, 'odo': 10, 'state': 'idle'}
'<<<'
'>>>'
asset-id: d230c0dc250dbd6c599bf684365231babbb3260d86d10b2e0ac0f570c292206f
metadata:
{'appended_at': 1523105242, 'odo': '0'}
'<<<'
kruistabel@kruistabel:~/bigchaindb$ python3 liftoff.py append --asset-id 681172b5702bc401b0e42d086d632f37cc9b266cdce95e9d1b8d48f84beb2563 --odo 10000
Append successful to asset-id '681172b5702bc401b0e42d086d632f37cc9b266cdce95e9d1b8d48f84beb2563'
kruistabel@kruistabel:~/bigchaindb$ python3 liftoff.py read --query tu_liftoff
results for query: 'tu_liftoff'
-------------------------------
asset
=====

'>>>'
asset-id: 681172b5702bc401b0e42d086d632f37cc9b266cdce95e9d1b8d48f84beb2563
data:
{'created_at': 1523105166,
 'index': '__liftoff__323e58f2f88da58e4dba8f35c4699823__tu__tu_liftoff',
 'object-id': 'tu_liftoff',
 'object-type': 'tu',
 'tu': {'batery_capacity': 100,
        'manufacturer': 'catan',
        'package_capacity': 2,
        'people_capacity': 4,
        'serial_number': 'QWERTY'}}
metadata:
{'batery_charge_level': 50, 'lat': 53, 'lon': 6.6, 'odo': 10, 'state': 'idle'}
'<<<'
'>>>'
asset-id: d230c0dc250dbd6c599bf684365231babbb3260d86d10b2e0ac0f570c292206f
metadata:
{'appended_at': 1523105242, 'odo': '0'}
'<<<'
'>>>'
asset-id: a5300168ac5af536e38ec16f618cc7e7ad653c379c5d68b88ca41417175ea712
metadata:
{'appended_at': 1523105259, 'odo': '10000'}
'<<<'
kruistabel@kruistabel:~/bigchaindb$ python3 liftoff.py append --asset-id 681172b5702bc401b0e42d086d632f37cc9b266cdce95e9d1b8d48f84beb2563 --odo 1000000 --burn
Append successful to asset-id '681172b5702bc401b0e42d086d632f37cc9b266cdce95e9d1b8d48f84beb2563'
kruistabel@kruistabel:~/bigchaindb$ python3 liftoff.py read --query tu_liftoff
results for query: 'tu_liftoff'
-------------------------------
asset
=====

'>>>'
asset-id: 681172b5702bc401b0e42d086d632f37cc9b266cdce95e9d1b8d48f84beb2563
data:
{'created_at': 1523105166,
 'index': '__liftoff__323e58f2f88da58e4dba8f35c4699823__tu__tu_liftoff',
 'object-id': 'tu_liftoff',
 'object-type': 'tu',
 'tu': {'batery_capacity': 100,
        'manufacturer': 'catan',
        'package_capacity': 2,
        'people_capacity': 4,
        'serial_number': 'QWERTY'}}
metadata:
{'batery_charge_level': 50, 'lat': 53, 'lon': 6.6, 'odo': 10, 'state': 'idle'}
'<<<'
'>>>'
asset-id: d230c0dc250dbd6c599bf684365231babbb3260d86d10b2e0ac0f570c292206f
metadata:
{'appended_at': 1523105242, 'odo': '0'}
'<<<'
'>>>'
asset-id: a5300168ac5af536e38ec16f618cc7e7ad653c379c5d68b88ca41417175ea712
metadata:
{'appended_at': 1523105259, 'odo': '10000'}
'<<<'
'>>>'
asset-id: 32d689e9769a4d40fd6428397101f19ceda622108c7df6c03d9cc6da3521b629
metadata:
{'burned_at': 1523105280}
'<<<'
kruistabel@kruistabel:~/bigchaindb$ python3 liftoff.py append --asset-id 681172b5702bc401b0e42d086d632f37cc9b266cdce95e9d1b8d48f84beb2563 --odo 1000000
Traceback (most recent call last):
  File "liftoff.py", line 189, in <module>
    cli()
  File "/usr/local/lib/python3.4/dist-packages/click/core.py", line 722, in __call__
    return self.main(*args, **kwargs)
  File "/usr/local/lib/python3.4/dist-packages/click/core.py", line 697, in main
    rv = self.invoke(ctx)
  File "/usr/local/lib/python3.4/dist-packages/click/core.py", line 1066, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/usr/local/lib/python3.4/dist-packages/click/core.py", line 895, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/usr/local/lib/python3.4/dist-packages/click/core.py", line 535, in invoke
    return callback(*args, **kwargs)
  File "liftoff.py", line 183, in append
    sent_transfer_tx = bdb.transactions.send(fulfilled_transfer_tx)
  File "/usr/local/lib/python3.4/dist-packages/bigchaindb_driver/driver.py", line 319, in send
    method='POST', path=self.path, json=transaction, headers=headers)
  File "/usr/local/lib/python3.4/dist-packages/bigchaindb_driver/transport.py", line 58, in forward_request
    headers=headers,
  File "/usr/local/lib/python3.4/dist-packages/bigchaindb_driver/connection.py", line 57, in request
    raise exc_cls(response.status_code, text, json)
bigchaindb_driver.exceptions.BadRequest: (400, '{\n  "message": "Invalid transaction (InvalidSignature): Transaction signature is invalid.", \n  "status": 400\n}\n', {'message': 'Invalid transaction (InvalidSignature): Transaction signature is invalid.', 'status': 400})
