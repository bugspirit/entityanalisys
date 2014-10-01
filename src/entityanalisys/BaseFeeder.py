import psycopg2
import sys
import datetime

from bitcoinrpc.authproxy import AuthServiceProxy

class BaseFeeder:
	def __init__(self, chainName, rpc, dbName, dbUser, dbPasswd, host='localhost'):
		try:
			self.dbConnect(dbName, dbUser, dbPasswd, host)
			self.getBitcoinDaemon(rpc)
			self.name = chainName
			self.createTables()
		except psycopg2.DatabaseError, e:
			print "Error %s", e
		# except:
			# print 'Unexpected Error'
			# exit(-1)
	
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
		
	def createTable_blocks(self):
		query = """
			CREATE TABLE blocks (
				blk_height integer PRIMARY KEY,
				blk_hash text NOT NULL,
				blk_ntx integer CHECK (blk_ntx>0),
				blk_time timestamp,
				blk_size integer CHECK (blk_size>0),
				blk_diff real CHECK (blk_diff>0));
		"""
		try:
			self.cur.execute(query)
		except psycopg2.DatabaseError, e:
			print 'Error %s' % e
			sys.exit(1)
			
	def createTable_transactions(self):
		query = """
			CREATE TABLE transactions (
				tx_id BIGSERIAL PRIMARY KEY,
				tx_hash varchar(75) NOT NULL UNIQUE,
				tx_amount integer,
				blk_id integer NOT NULL CHECK (blk_id>0),
				ent_id integer,
				coinbase boolean,
				weird boolean,
				vins integer CHECK (vins>0),
				vouts integer CHECK (vouts>0)
				);
		"""
		try:
			self.cur.execute(query)
		except psycopg2.DatabaseError, e:
			print 'Error %s' % e
			sys.exit(1)
			
	def createTable_inputs(self):
		query = """
			CREATE TABLE inputs (
				inp_id BIGSERIAL PRIMARY KEY,
				inp_address text,
				tx_hash varchar(75) NOT NULL,
				tx_vout integer NOT NULL,
				inp_amount integer CHECK (inp_amount>0),
				coinbase boolean,
				spent boolean
				);
		"""
		try:
			self.cur.execute(query)
		except psycopg2.DatabaseError, e:
			print 'Error %s' % e
			sys.exit(1)
			
	def createTable_entities(self):
		query = """
			CREATE TABLE entities (
				ent_id integer PRIMARY KEY,
				num_addresses integer CHECK (num_addresses>0),
				ent_comment text,
				ent_firstAppear timestamp NOT NULL
				);
		"""
		try:
			self.cur.execute(query)
		except psycopg2.DatabaseError, e:
			print 'Error %s' % e
			sys.exit(1)
			
	def createTable_addresses(self):
		query = """
			CREATE TABLE addresses (
				addr_id BIGSERIAL PRIMARY KEY,
				addr_key text NOT NULL,
				addr_balance real CHECK (addr_balance>0),
				ent_id bigint NULL,
				addr_firstAppear timestamp,
				tx_hash varchar(75)
				);
		"""
		try:
			self.cur.execute(query)
		except psycopg2.DatabaseError, e:
			print 'Error %s' % e
			sys.exit(1)
			
	def createTable_processingInfo(self):
		pass
			
	def tableExists_blocks(self):
		query = ''' SELECT table_name FROM information_schema.tables WHERE table_name = 'blocks'; '''
		self.cur.execute(query)
		return bool(self.cur.rowcount)
	
	def tableExists_transactions(self):
		query = ''' SELECT table_name FROM information_schema.tables WHERE table_name = 'transactions'; '''
		self.cur.execute(query)
		return bool(self.cur.rowcount)
		
	def tableExists_inputs(self):
		query = ''' SELECT table_name FROM information_schema.tables WHERE table_name = 'inputs'; '''
		self.cur.execute(query)
		return bool(self.cur.rowcount)
	
	def tableExists_addresses(self):
		query = ''' SELECT table_name FROM information_schema.tables WHERE table_name = 'addresses'; '''
		self.cur.execute(query)
		return bool(self.cur.rowcount)
		
	def tableExists_entities(self):
		query = ''' SELECT table_name FROM information_schema.tables WHERE table_name = 'entities'; '''
		self.cur.execute(query)
		return bool(self.cur.rowcount)
	
	def createTables(self):
		if not self.tableExists_blocks():
			self.createTable_blocks()
		if not self.tableExists_transactions():
			self.createTable_transactions()
		if not self.tableExists_entities():
			self.createTable_entities()
		if not self.tableExists_addresses():
			self.createTable_addresses()
				
	def getBlockByIndex(self,index):
		return self.daemon.getblock(self.daemon.getblockhash(index))
				
	def getBitcoinDaemon(self, rpc):
		try:
			self.daemon = AuthServiceProxy(rpc)
		except socket.error, e:
			print 'Could not connect to daemon!   @ %s' % rpc #censure the password!
			return None
		except httplib.BadstatusLine, e:
			print 'Could not connect to daemon'
			return None

	def writeBlock2db(self, index):
		block = self.getBlockByIndex(index)
		
		height = block['height']
		ntx    = len(block['tx'])
		hash   = block['hash']
		size   = block['size']
		time   = block['time']
		diff   = block['difficulty']
		
		try:
			template = """
			INSERT INTO blocks (blk_height, blk_ntx, blk_hash, blk_size, blk_time, blk_diff)
			VALUES (%s,%s,%s,%s,%s,%s)
			"""
			query = self.cur.mogrify( template,(height,ntx,hash,size,datetime.datetime.fromtimestamp(time),diff,) ) 
			self.cur.execute(query)
		except psycopg2.ProgrammingError, e:
			print "Block %d wasnt written to DB" % index
			print "Error :: ", e
			return False
		except psycopg2.IntegrityError, e:
			print "Lost integrity of DB. Bad request"
			print "Block :: %d" % index
			return False
		else:
			return True

	def writeManyBlocks2DB(self, start, finish):
		if (start>finish):
			raise Exception("Failed to write many blocks :: impossible conditions", start, finish)
		for i in range(start,finish):
			self.writeBlock2db(i)
			print "Block #%d successfully written" % i
			self.con.commit()

	def writeBlocksFrom(self, start):
		n2 = self.daemon.getinfo()['blocks']
		n  = start
		while (n<n2):
			self.writeManyBlocks2DB(n+1,n2)
			n=n2
			n2 = self.daemon.getinfo()['blocks']

	def updateAllBlocksInDB(self):
		query = """SELECT MAX(blk_height) FROM blocks"""
		self.cur.execute(query)
		start = self.cur.fetchone()[0]
		writeBlocksFrom(start)

	def getRawTransactionInfo(self, txid):
		return self.daemon.decoderawtransaction(self.daemon.getrawtransaction(txid))
		
	def getTransactionInfo(self,txid):
		tx = self.getRawTransactionInfo(txid)
		r = {}
		
		r['vins'] = len( tx['vin'] )
		r['vouts'] =len( tx['vout'] )
		r['coinbase'] = ( 'coinbase' in tx['vin'][0].keys() )
		amount = 0
		for v in tx['vout']:
			amount += v['value']
		r['amount'] = amount
		return r	
		
	def writeTxFromBlock(self, index):
		blockInfo = self.getBlockByIndex(index)
		template = '''
			INSERT INTO transactions (vins, vouts, tx_hash, coinbase, tx_amount, blk_id)
			VALUES (%s,%s,%s,%s,%s,%s);
		'''
		for hash in blockInfo['tx']:
			txinfo = self.getTransactionInfo(hash)
			blk_id = blockInfo['height']
			try: 
				query = self.cur.mogrify(template, ( txinfo['vins'], txinfo['vouts'], hash, txinfo['coinbase'], txinfo['amount'], blk_id, ) )
				self.cur.execute(query)
			except psycopg2.IntegrityError, e:
				print "Transaction integrity fault! Hash: %s, Block %d" % (hash, blockInfo['height'])
				template = '''
						INSERT INTO transactions (vins, vouts, tx_hash, coinbase, tx_amount, blk_id, weird)
						VALUES (%s,%s,%s,%s,%s,%s, %s);
							'''
				query = self.cur.mogrify(template, ( txinfo['vins'], txinfo['vouts'], hash, txinfo['coinbase'], txinfo['amount'], blk_id, True, ) )
				self.cur.execute(query)
				template = '''
					INSERT INTO transactions (vins, vouts, tx_hash, coinbase, tx_amount, blk_id)
					VALUES (%s,%s,%s,%s,%s,%s);
						'''
		ntx = len( blockInfo['tx'] )
		if ntx>1:
			pass
			#print "Block %d processed. %d transactions successfully written" % (index, ntx )
		else:
			pass
			#print "Block %d processed. 1 transaction successfully written" % (index)		
		self.con.commit()
		return len(blockInfo['tx'])
		
	def writeTxFromManyBlocks(self, start, finish):
		if (start<=0) or (finish<start):
			raise Exception("Failed to write many block. Impossible conditions ", start, finish)
		for i in range(start, finish):
			self.writeTxFromBlock(i)
		
	def updateAllTx2DB(self):
		query = '''SELECT MAX(blk_id) FROM transactions'''
		self.cur.execute(query)
		blocksInDB  = self.cur.fetchone()[0]
		if blocksInDB != None:
			query = ''' DELETE FROM transactions WHERE blk_id=%s ;''' % str(blocksInDB)
			self.cur.execute(query)
		else:
			blocksInDB = 1
		blocksTotal = self.daemon.getinfo()['blocks']
		if (blocksInDB<blocksTotal):
			self.writeTxFromManyBlocks( blocksInDB, blocksTotal)

	def writeTxInputs(self,txid):
		tx = self.getRawTransactionInfo(txid)
		if not ('coinbase' in tx['vin'][0].keys() ):
			template = '''
					INSERT INTO inputs (inp_address, tx_id, tx_vout, coinbase)
							VALUES (%s, %s, %s, %s);
				'''
			for input in tx['vin']:
				vout = input['vout']
				txid = input['txid']
				d = self.getRawTransactionInfo(daemon, txid)
				addr = d['vout'][vout]['scriptPubKey']['addresses'][0]
				
				try:
					query = self.cur.mogrify(template, ( addr, txid, vout, False, ) )
					self.cur.execute(query)
				except:
					print 'Unknown (for now) Error.  txid :: %s    input :: %d' % (txid, vout)
		else:
			template = '''
				INSERT INTO inputs (tx_id, tx_vout, coinbase)
						VALUES (%s, %s, %s);
			'''
			try:
				query = self.cur.mogrify(template, (txid, 1, True, ) )
				self.cur.execute(query)
			except:
				print 'Unknown (for now) Error.  txid :: %s    input :: %d' % (txid, vout)
		print 'Transaction %s successfully processed.  %d inputs written to DB' % (txid, len(tx['vin']))
		self.con.commit()
		
	def writeTxInputsFromBlock(self, index):
		blockInfo = self.getBlockByIndex(index)
		num = 0
		for txid in blockInfo['tx']:
			self.writeTxInputs(txid)
		print 'Block %d successfully processed' % index
				
	def writeTxInputsFromManyBlocks(self, start, finish):
		for i in range(start, finish):
			self.writeTxInputsFromBlock(i)
			
	def checkRange(start, finish):
		return ( (start>0) and (start<=finish) )
				
	def getAddressesFromTx(self, txid):
		addr = []
		data = self.getRawTransactionInfo(txid)
		
		if not( 'coinbase' in data['vin'][0].keys() ):
			for input in data['vin']:
				txin = input['txid']
				vout = input['vout']
				data = self.getRawTransactionInfo(txin)
				try:
					addr.append( data['vout'][vout]['scriptPubKey']['addresses'][0])
				except KeyError:
					print 'WEIRD  %s' % txid
					return False
		return addr
		
	def writeAddressesFromTx(self, txid):
		addresses = self.getAddressesFromTx(txid)
		if not addresses:
			return False                                                      # HANDLE THIS
		else:
			template = """
			INSERT INTO addresses (tx_hash, addr_key)
						VALUES(%s, %s);
			"""
			for addr in addresses:
				try:
					query = self.cur.mogrify(template, (txid, addr, ) )
					self.cur.execute(query)
				except:
					print "Unnown DB error   txid:: %s    addr :: %s" % (txid, addr)
			self.con.commit()		
			return len(addresses)
				
	def writeAddressesFromBlock(self, index):
		blockInfo = self.getBlockByIndex(index)
		n = 0
		for hash in blockInfo['tx']:
			w = self.writeAddressesFromTx(hash)
			if w:
				n += w
		#print "Block %d processed. Addresses from %d transactions written" % (index, len(blockInfo['tx']) )
		return n
		
	def writeAddressesFromManyBlocks(self, start, finish):
		if self.checkRange(start, finish):
			for i in range(start, finish):
				self.writeAddressesFromBlock(i)
		else:
			raise Exception("Wrong range! ", start, finish)
			
	def updateAllAdressesInDB(self):
		query = '''SELECT MAX(blk_id) FROM transactions'''
		self.cur.execute(query)
		blocksInDB  = self.cur.fetchone()[0]
		if blocksInDB != None:
			query = ''' DELETE FROM addresses WHERE blk_id=%s ;''' % str(blocksInDB)
			self.cur.execute(query)
		else:
			blocksInDB = 1
		blocksTotal = self.daemon.getinfo()['blocks']
		if (blocksInDB<blocksTotal):
			self.writeTxFromManyBlocks(blocksInDB, blocksTotal)
			
	def cleverUpdateDB(self):
		print 'Cleaning up........'
		query = '''SELECT MAX(blk_id) FROM transactions;'''
		self.cur.execute(query)
		blocksInDB  = self.cur.fetchone()[0]
		if blocksInDB != None:
			template = ''' DELETE FROM addresses WHERE tx_hash IN (SELECT tx_hash FROM transactions WHERE blk_id = %s);''' 
			query = self.cur.mogrify(template, (blocksInDB,) )
			self.cur.execute(query)
			query = ''' DELETE FROM transactions WHERE blk_id=%s ;''' % str(blocksInDB)
			self.cur.execute(query)
			query = ''' DELETE FROM blocks WHERE blk_height=%s;''' % str(blocksInDB)
			self.cur.execute(query)
			self.con.commit()
		else:
			blocksInDB = 1
		blocksTotal = self.daemon.getinfo()['blocks']
		if (blocksInDB<blocksTotal):
			print "GEtting some numbers from DB......."
			query = ''' SELECT count(*) FROM transactions; '''
			self.cur.execute(query)
			txW = self.cur.fetchone()[0]
			query = ''' SELECT count(*) FROM addresses; '''
			self.cur.execute(query)
			aW = self.cur.fetchone()[0]
			for i in range(blocksInDB, blocksTotal):
				self.writeBlock2db(i)
				txW += self.writeTxFromBlock(i)
				aW  += self.writeAddressesFromBlock(i)
				self.con.commit()
				print 'Block %d processed ' % i
				print "%d transactions    %d addresses written" % (txW, aW)