from bisect import bisect_left, insort_left
import anydbm

def index(a, x):
    'Locate the leftmost value exactly equal to x'
    i = bisect_left(a, x)
    if i != len(a) and a[i] == x:
        return True
    return False

class DisjointSet:
	maxnum = None
	
	def __init__(self, many=False):
		self.skeys = []
		if not many:
			self.keys  = {}
			self.bkeys = {}
			self.parents  = {}
		
	def addAddress(self, addr, key, parent=None):
		if not self.maxnum:
			self.maxnum = addr
			self.addAddressCheap(addr,key,parent)
		
		if ( addr<self.maxnum ) or ( index(self.skeys, key) ):
			return False
		else:
			self.keys[addr] = key
			self.bkeys[key] = addr
			self.maxnum = addr
			insort_left(self.skeys,key)
			if parent:
				self.parents[addr] = parent
			else:
				self.parents[addr] = addr
			return True
		
	def addAddressCheap(self, addr, key, parent=None):
		#use if sure that addr is NOT in sets
		self.keys[addr] = key
		self.bkeys[key] = addr
		self.maxnum = addr
		insort_left(self.skeys,key)
		if parent:
			self.parents[addr] = parent
		else:
			self.parents[addr] = addr
				
	def FindEntity(self,xx):
		if type(xx) is str:
			num = self.bkeys[xx]
		else:
			num = xx
		
		if self.parents[num] == num:
			return num
		else:
			return self.FindEntity( self.parents[num] )
	
	def findAddrByKey(self,key):
		try:
			return self.bkeys[key]
			# BINARY SEARCH ON SKEYS   !!!
		except KeyError:
			return False
	
	def mergeEntities(self, a, b):
		try:
			if a == b:
				return
			elif a < b:
				enta = self.FindEntity(a)
				entb = self.FindEntity(b)
				self.parents[entb] = enta
			else:
				enta = self.FindEntity(a)
				entb = self.FindEntity(b)
				self.parents[enta] = entb
		except KeyError:
			raise Exception("DSet Merge :: No SUch entity!")
			
	def compressTree(self):
		for addr in self.keys.keys():
			self.parents[addr] = self.FindEntity(addr)
			
	def printAll(self):
		for addr in self.parents.keys():
			print 'Addr :: %d   Parent :: %d  Key :: %s ' % ( addr, self.parents[addr], self.keys[addr] )
		print
		
	def isNotEmpty(self):
		if self.keys:
			return True
		else:
			return False
			
	def size(self):
		return len(self.keys)
		
if __name__ == '__main__':
	ds = DisjointSet()
	for i in range(1,20):
		ds.addAddress(i,'fffffffffff%s' % str(i))
			
	ds.printAll()
	ds.mergeEntities(1,2)
	ds.mergeEntities(3,4)
	ds.printAll()
	ds.mergeEntities(1,4)
	ds.mergeEntities(4,12)
	ds.printAll()
	
	print ds.skeys
	print ds.addAddress(1, 'rrrrrrrr')
	ds.compressTree()
	ds.printAll()
	