import DisjointSet
import psycopg2

rpc = 'http://walter:lenin@127.0.0.1:8332/'
DB = 'Test'
user = 'Tester'
pwd = 'infiltractor'
DBhost = 'Alex3'

class DBEntry:
	num =  None
	key =  None
	txid = None
	
	def __init__(self, num, key, txid):
		if (type(num) is int) or (type(num) is long):
			self.num = num
		if type(key) is str:
			self.key = key
		if type(txid) is str:
			self.txid = txid
		if not (self.num and self.key and self.txid):
			self.printContents()
			raise Exception('DBEntry :: wrong construct')
			
	def printContents(self):
		print 'DBE  num :: %d' % self.num
		print 'DBE  key :: %s' % self.key
		print 'DBE txid :: %s' % self.txid

class AnalyseEngine:
	__ab = None
	__djs = None
	toDelete = []
	
	def __init__(self, name, DB, user, pwd, DBhost='localhost', noWrite=False):
		self.__ab = AnalyseBase(DB, user, pwd, DBhost)
		self.__djs = DisjointSet.DisjointSet(name)
		self.noWrite = noWrite
	
	def isReady(self):
		ready = self.__ab.ifAddressesTableNotEmpty()
		return ready
		
	def process(self, n, all=False):
		if not self.__djs.isNotEmpty():
			self.readProcessed()
			
		print 'Connecting to base....'
		cursor = self.__ab.getCursor2Bunch(n, all)
		res = cursor.fetchall()
		print 'Data accepted'
		
		if not res:
			raise Exception('AE:: Could not fetch data from database')
		
		#get address to confortable
		entries = []
		for addr in res:
			try:
				e = DBEntry(addr[0], addr[1], addr[2])
				entries.append(e)
			except:
				print 'Wrong DBE construct %d, %s , %s' % ( addr[0], addr[1], addr[2] )
		del res
		
		#adding addresses to sets
		d=0
		i=0
		print 'Adding addresses to sets'
		for entr in entries:
			i += 1
			if not self.__djs.addAddress( entr.num, entr.key ):
				self.toDelete.append( entr.num )
				d += 1
			if i%100 == 0:
				print 'Added %d addresses, %d known' % (i,d)
		print
		
		#processing DJS
		first = entries.pop()
		i=0
		l = self.__djs.size()
		while entries:
			current = entries.pop()
			i += 1
			while current.txid == first.txid:
				try:
					enta = self.__djs.FindEntity(current.key)
					entb = self.__djs.FindEntity(  first.key)
					self.__djs.mergeEntities(enta, entb)
					current = entries.pop()
					i += 1
					if abs( enta - entb ) > 50:
						print "Found distant :: %d  %d" % (enta, entb)
				except IndexError:
					break
				else:
					if i%100 == 0:
						print 'Processed %d adresses. %d in sets' % (i, l)
			first = current
		
		self.__djs.compressTree()
		print
		
	def readProcessed(self):
		cursor = self.__ab.readProcessed()
		processed = cursor.fetchall()
		print 'Reading previously processed addresses'
		i = 0
		for addr in processed:
			self.__djs.addAddress(addr[0], addr[1], addr[2])
			i += 1
			if i%1000 == 0:
				print 'Read %d previously processed addresses to sets' % i
		print
		
	def printSets(self):
		self.__djs.printAll()
		
	def commit2Base(self):
		if self.noWrite:
			print "Dont write anything to Base"
			return
		self.__ab.deleteAdresses(self.toDelete)
		
		#self.toDelete = []
		i=0
		l = self.__djs.size()
		for addr in self.__djs.parents.keys():
			try:
				self.__ab.writeEntity(addr, self.__djs.parents[addr])
			except psycopg2.InternalError:
				import sys
				sys.sleep(100)
				self.__ab.writeEntity(addr, self.__djs.parents[addr])
			if i%100 == 0:
				print 'Writing entities to base. %d of %d done' % (i,l)
			i+=1
		self.__ab.commit()
		
class AnalyseBase:
	def __init__(self, dbname, username, passwd, hostname='localhost' ):
		self.dbConnect(dbname, username, passwd, hostname)
		
	def dbConnect(self, dbname, username, passwd, hostname='localhost' ):
		con = None
		try:
			self.con = psycopg2.connect(database=dbname, user=username, password=passwd, host=hostname)
			self.cur = self.con.cursor()
		except psycopg2.DatabaseError, e:
			print "Error %s", e
			if self.con:
				self.con.close()
			exit(1)	
	
	def ifAddressesTableNotEmpty(self):
		query = ''' SELECT * FROM addresses LIMIT 1 '''
		self.cur.execute(query)
		res = self.cur.fetchone()
		if not res:
			return False
		else:
			return True
					
	def getBunch(self, n, all=False):
		if n<5:
			raise Exception('Wrong n in getBunch function in AnalyseBase class N>=5')
			return None
		if not all:
			query = ' SELECT addr_id, addr_key, tx_hash FROM addresses WHERE ent_id IS NULL ORDER BY addr_id LIMIT %s; ' % str(n)
		else:
			query = ' SELECT addr_id, addr_key, tx_hash FROM addresses WHERE ent_id IS NULL ORDER BY addr_id;'
		try:
			self.cur.execute(query)
			res = self.cur.fetchall()
			lasttx = res.pop(n-1)[3]
		except psycopg2.DatabaseError, e:
			print "Cannot request addresses from DB %s " % e
			return False
		except IndexError:
			print "Base returned empty response"
			print query
		else:
			# delete last transaction to maintain integrity
			i = n-2
			while res[i][3] == lasttx:
				res.pop(i)
				i -= 1
			return res
		
	def getCursor2Bunch(self, n, all=False):
		if not all:
			query = ' SELECT addr_id, addr_key, tx_hash FROM addresses WHERE ent_id IS NULL ORDER BY addr_id LIMIT %s; ' % str(n)
		else:
			query = ' SELECT addr_id, addr_key, tx_hash FROM addresses WHERE ent_id IS NULL ORDER BY addr_id;'
		# self.cur.execute(query)
		# return self.cur
		try:
			self.cur.execute(query)
		except:
			print 'GetCursor :: something wrong'
			return
		else:
			return self.cur
	
	def flushDB(self):
		pass
		
	def deleteAdresses(self, marked):
		try:
			query = 'DELETE FROM addresses WHERE addr_id in ( '
			while marked:
				s = str(marked.pop())
				query += s + ', '
			# for num in marked:
				# query += str(num) + ', '
			query += s + ');'
			
			self.cur.execute(query)
			#self.con.commit()
		except IndexError:
			return
		except:
			"Something strange while deleting dublicate addresses"
	
	def writeEntity(self, addr, ent):
		query = 'UPDATE addresses SET ent_id = %s WHERE addr_id = %s' % ( str(ent), str(addr) )
		self.cur.execute(query)
	
	def addressesinDB(self):
		query = 'SELECT count(*) FROM addresses;'
		self.cur.execute(query)
		return self.cur.fetchall()[0][0]
		
	def readProcessed(self):
		#returns psycopg cursor
		query = 'SELECT addr_id, addr_key, ent_id FROM addresses WHERE ent_id IS NOT NULL ORDER BY addr_id;'
		self.cur.execute(query)
		return self.cur
	
	def commit(self):
		self.con.commit()
