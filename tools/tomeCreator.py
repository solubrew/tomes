#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@||
'''  #																			||
---  #																			||
<(META)>:  #																	||
	docid: ''  #							||
	name: ''  #						||
	description: >  #															||
	expirary: <[expiration]>  #													||
	version: <[version]>  #														||
	authority: document|this  #													||
	security: sec|lvl2  #														||
	<(WT)>: -32  #																||
''' #																			||
# -*- coding: utf-8 -*-#														||
#================================Core Modules===================================||
from sys import argv
from os import listdir
from os.path import abspath, dirname, exists, isfile, join
#===============================================================================||
from pandas import DataFrame
#===============================================================================||
import crow
#===============================================================================||
from condor import condor
from excalc import tree as calctr
from fxsquirl.orgnql import fonql, yonql
from fxsquirl import collector
#===============================================================================||
here = join(dirname(__file__),'')#												||
there = abspath(join('../../..'))#												||set path at pheonix level
version = '0.0.0.0.0.0'#														||
log = True
#===============================================================================||
pxcfg = join(abspath(here), '_data_/tomeCreator.yaml')
def run(seq):
	''' '''
	if seq[1] in ('0', 'all'):
		createTokenTomes()
	elif seq[1] in ('1', 'all'):
		createNFTTomes()
	elif seq[1] in ('2'):
		buildTomesList()


def buildTomesList():
	''' '''
	tomes = loadTokens()
	path = join(abspath(here), f'..')
	blooms = listdir(f'{path}')
	for bloom in blooms:
		if isfile(f'{path}/{bloom}') or len(bloom) > 1: continue
		if log: print('Bloom', bloom)
		for tome in listdir(f'{path}/{bloom}'):
			if not isfile(f'{path}/{bloom}/{tome}'): continue
			if log: print('Tome', tome)
			tomeD = condor.instruct(f'{path}/{bloom}/{tome}').load().dikt
			symbol = tomeD['token']['1']['symbol']
			if 'allow' not in tomes[symbol].keys():
				tomes[symbol]['allow'] = False
			if 'proxyFor' not in tomes[symbol].keys():
				tomes[symbol]['proxyFor'] = None
			if 'family' not in tomes[symbol].keys():
				tomes[symbol]['family'] = None
	f = join(abspath(here), f'../tokens.yaml')
	yonql.doc(f).write(tomes)


def createTokenTomes(db=False):
	''' '''
	tomes = loadTokens()
	db = True
	if db:
		data = loadDB()
		if log: print(data)
		tomes = {x: {'allow': True} for x in data.symbol.unique()}
		if log: print('Tomes', tomes)
	createTomes(tomes, data)


def createNFTTomes():
	''' '''
	tomes = loadNFTs()
	createTomes(tomes)


def createTomes(tomes, df=DataFrame()):
	''' '''
	cfg = condor.instruct(pxcfg).select('tomeTMPLT').dikt
	if log: print('Cfg', cfg)
	for tome in tomes.keys():
		if not tome: continue
		bloom = tome.lower()[:1]
		f = join(abspath(here), f'../{bloom}/tome{tome}.yaml')
		fonql.touch(f)
		doc = condor.instruct(f).load().dikt
		#doc = {}

		data = cfg['data']
		data['status'] = tomes[tome]['allow']
		try:
			if tomes[tome]['address']:
				data['token']['1']['address'] = str(tomes[tome].get('address'))
		except:
			pass
		if not df.empty:
			row = df[df['symbol'] == tome].values.tolist()[0]
			if log: print('Row', row)
			data['token']['1']['address'] = row[4]
			data['token']['1']['name'] = row[1]
			data['token']['1']['description'] = row[5]
		data['token']['1']['symbol'] = tome
		data['family'] = tomes[tome].get('family')
		data['children'] = findFamily(tome, tomes)
		data['proxyFor'] = tomes[tome].get('proxyFor')
		try:
			if tomes[tome]['chainId']:
				data['token']['1']['chainId'] = tomes[tome].get('chainId')
		except:
			pass
		data['proxyFor'] = tomes[tome].get('proxyFor')
		yonql.doc(f).write(calctr.merger(doc, data, 'override'))


def findFamily(symbol, tomes):
	''' '''
	fam = []
	for tome in tomes.keys():
		parent = tomes[tome].get('family')
		if parent == symbol:
			fam.append(tome)
	return fam


def loadTokens():
	''' '''
	f = join(abspath(here), f'../tokens.yaml')
	return condor.instruct(f).load().dikt


def loadNFTs():
	''' '''
	f = join(abspath(here), f'../nfts.yaml')
	return condor.instruct(f).load().dikt


def loadDB(sdb=0):
	'''
		Get tokens data table
	'''
	if sdb == 0:
		db = 'BlockchainDR:ethereum'
	elif sdb == 1:
		db = 'EconometricDR:crypto'
	table = 'tokens'
	data = collector.engine().initSource(db, 'db', table)
	data.setReader({'table': [table]}, table)
	data.initExtract(['symbol', 'address', 'name'], 'dataframe', table)
	data.extract(table)
	return data.cache.store[table]


if __name__ == '__main__':
	run(argv)
