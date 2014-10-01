from entityanalisys import BaseFeeder, AnalyseEngine

rpc = 'http://walter:lenin@127.0.0.1:8332/'
DB = 'Test'
user = 'Tester'
pwd = 'infiltractor'
DBhost= 'ALEX3'

pci = BaseFeeder.BaseFeeder('Bitcoin', rpc, DB, user, pwd, DBhost)
pci.cleverUpdateDB()