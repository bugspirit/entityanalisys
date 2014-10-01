from entityanalisys import BaseFeeder, AnalyseEngine

rpc = 'http://walter:lenin@127.0.0.1:5888/'
DB = 'VertcoinDB'
user = 'Verter'
pwd = 'vertcoiner'
host= 'ALEX3'

B = BaseFeeder.BaseFeeder('Vertcoin', rpc, DB, user, pwd, host)
B.cleverUpdateDB()
A = AnalyseEngine.AnalyseEngine('Vertcoin' ,DB, user, pwd, host)
A.process(1, all=True)
# print 'Processed. Writing to Base'
A.commit2Base()