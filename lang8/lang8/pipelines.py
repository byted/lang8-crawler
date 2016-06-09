# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import json
import codecs
import re
import os
from scrapy import signals
from scrapy.exceptions import DropItem

class Lang8Pipeline(object):
	def __init__(self):
		self.output_directory = 'output2'
		self.filename = ''
		self.file_handle = {}
		self.data = {}

	@classmethod
	def from_crawler(cls, crawler):
		pipeline = cls()
		crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
		crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
		return pipeline

	def spider_opened(self, spider, fileName='default.json'):
		if not os.path.exists(self.output_directory):
			os.makedirs(self.output_directory)
		self.filename = fileName
		file = codecs.open(self.output_directory + self.filename, 'wb', encoding="utf-8")
		self.file_handle[fileName] = file
		self.data[fileName] = file


	def process_item(self, item, spider):
		fname='default'
		if not item['correction']:
			raise DropItem("No Correction Scraped in %s" % item['url'])
		catname = re.search(r'http://lang-8.com/(\d+)/journals/(.*?)', str(item['url']), re.I)
		if catname:
			fname = catname.group(1)
		self.curFileName = fname + ".json"

		if self.filename=='default.json':
			if os.path.isfile(self.output_directory +self.filename):
				os.rename(self.output_directory + self.filename, 'output2/' + self.curFileName)
			file = self.file_handle['default.json']
			del self.file_handle['default.json']
			self.file_handle[self.curFileName] = file
			self.filename = self.curFileName
		if self.filename != self.curFileName and not self.file_handle.get(self.curFileName):
			self.spider_opened(spider, self.curFileName)

		if self.curFileName not in self.data:
			self.data[self.curFileName] = []
		self.data[self.curFileName].append(dict(item))
		return item

	def spider_closed(self, spider):
		for file_name, fileH in self.file_handle.iteritems():
			fileH.write(json.dumps(self.data[file_name]))
			fileH.close()
