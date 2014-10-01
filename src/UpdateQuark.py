from entityanalisys import BaseFeeder, AnalyseEngine

rpc = 'http://walter:lenin@127.0.0.1:8217/'
DB = 'QuarkTest'
user = 'Quarker'
pwd = 'infiltractor'
host= 'ALEX3'

A = AnalyseEngine.AnalyseEngine('Quark', DB, user, pwd, host)
B = BaseFeeder.BaseFeeder('QuarkCoin', rpc, DB, user, pwd, host)
B.cleverUpdateDB()
A.process(1, all=True)
# print 'Processed. Writing to Base'
A.commit2Base()

