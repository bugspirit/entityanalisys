import BaseFeeder

rpc = 'http://walter:lenin@127.0.0.1:8217/'
DB = 'QuarkTest'
#DB = 'QuarkRaw'
user = 'Quarker'
pwd = 'infiltractor'
host= 'ALEX3'

pci = BaseFeeder.BaseFeeder('QuarkCoin', rpc, DB, user, pwd, host)
pci.cleverUpdateDB()