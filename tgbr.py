#!/usr/bin/env python  
#-*-coding:utf-8 -*-        
import anydbm
import re
import math
from myzodb import MyZODB, transaction

class TGBR:	

	MusicSet = {}
	InvertedList = {}

	#gview有向图模型
	class gview:

		def __init__(self,tagMat):
			self.nodes = {}
			self.edges = {}
			for tags in tagMat:
				for i in range(len(tags)):
					if self.nodes.has_key(tags[i]):
						self.nodes[tags[i]] += 1
					else:
						self.nodes[tags[i]] = 1

					if i == len(tags)-1:continue
					key = tags[i]+','+tags[i+1]
					if self.edges.has_key(key):
						self.edges[key] += 1
					else:
						self.edges[key] = 1
		#合并有向图，用于聚类中心的计算
		def merge(self,mainGraph,subGraph):
			for i in subGraph.nodes:
				if mainGraph.nodes.has_key(i):
					mainGraph.nodes[i] += subGraph.nodes[i]
				else:
					mainGraph.nodes[i] = subGraph.nodes[i]
			for j in subGraph.edges:
				if mainGraph.edges.has_key(j):
					mainGraph.edges[j] += subGraph.edges[j]
				else:
					mainGraph.edges[j] = subGraph.edges[j]
			return mainGraph

		
	def __init__(self):
		self.uri2name = anydbm.open('E:\\sam_work\\pagerank\\pr\\uri2name.db','r')
		self.yc = anydbm.open('E:\\sam_work\\pagerank\\pr\\yanchang.db','r')
		self.id2name = anydbm.open('id2name.db','r')


	#计算两个有向图的同构度
	def CalSim(self,graph1,graph2):
		#计算子图与父图的同构度
		def Calfai(self,fview_nodes,fview_edges,gview):			
			nodes_fview = 0
			for node in fview_nodes:
				nodes_fview += float(gview.nodes[node])
			if len(gview.nodes) == 0:nodes_gview = 1
			else:nodes_gview = float(sum(gview.nodes.values()))			
			edges_fview = 0
			for edge in fview_edges:
				edges_fview += float(gview.edges[edge])
			if len(gview.edges) == 0:edges_gview = 1
			else:edges_gview = float(sum(gview.edges.values()))
			
			nodes = float(len(fview_nodes))
			edges = float(len(fview_edges))
			outvalue = nodes*(nodes_fview/nodes_gview) + edges*(edges_fview/edges_gview)
			
			return outvalue
		#发现子图
		def Findfview():
			fview_nodes = []
			fview_edges = [] 
			for i in graph1.edges:
				if graph2.edges.has_key(i):
					fview_edges.append(i)
			for j in graph1.nodes:
				if graph2.nodes.has_key(j):
					fview_nodes.append(j)
			return fview_edges,fview_nodes


		fview_edges,fview_nodes = Findfview()
		outvalue1 = Calfai(self,fview_nodes,fview_edges,graph1)
		outvalue2 = Calfai(self,fview_nodes,fview_edges,graph2)
		outvalue = math.sqrt(outvalue1**2 + outvalue2**2)
		return outvalue


	#从MODB中读取聚类的数据
	def loadingMODB(self,DBFilePath):
		db = MyZODB(DBFilePath)
		dbrootN = db.dbroot
		for t in dbrootN['InvertedList']:
			if t == '':continue
			temphash = dbrootN['InvertedList'][t]
			keys = sorted(temphash.iteritems(),key=lambda temphash:temphash[1],reverse=True)
			c = 0
			temphash2 = {}
			for j in keys:
				c += 1
				if c > 1000:break
				temphash2[j[0]] = j[1]
			self.InvertedList[t] = temphash2
		for sl in dbrootN['gviews']:
			self.MusicSet[sl] = self.gview([])
			for node in dbrootN['gviews'][sl]['nodes']:
				self.MusicSet[sl].nodes[node] = dbrootN['gviews'][sl]['nodes'][node]
			for edge in dbrootN['gviews'][sl]['edges']:
				self.MusicSet[sl].edges[edge] = dbrootN['gviews'][sl]['edges'][edge]
		transaction.commit()
		db.close()
		return self.MusicSet,self.InvertedList







	#模型构建
	def traning(self,db):
		#读入歌单数据
		def LoadingData(self,testnum):
			self.gUserBase = {}
			self.gMusicBase = {}	
			totalTags = {}
			c = 0
			for i in db:
				c += 1
				if c >= testnum:break
				tempmat = re.compile(r'\|').split(db[i])
				if len(tempmat[1]) == '':continue
				author = tempmat[0]
				tags = re.compile(',').split(tempmat[1])
				if len(tags) == 0:continue
				for t in tags:totalTags[t] = ''
				#print ','.join(tags)
				songs = re.compile(',').split(tempmat[2])
				if author != 'null':
					if self.gUserBase.has_key(author):
						self.gUserBase[author].append(tags)
					else:
						self.gUserBase[author] = []
						self.gUserBase[author].append(tags)

				for song in songs:
					if self.gMusicBase.has_key(song):
						self.gMusicBase[song].append(tags)
					else:
						self.gMusicBase[song] = []
						self.gMusicBase[song].append(tags)
			print 'initial process completed!   ',len(self.gUserBase),'users,',len(self.gMusicBase),'musics!	',len(totalTags),'tags'
		#创建标签序列集合
		def GetGraphSet(self):
			self.gSetToUser = {}
			self.gSetToMusic = {}
			
			#gSetToUser
			for i in self.gUserBase:
				tagsMat = self.gUserBase[i]
				self.gSetToUser[i] = self.gview(tagsMat)
			for j in self.gMusicBase:
				tagsMat = self.gMusicBase[j]
				self.gSetToMusic[j] = self.gview(tagsMat)
		#聚类
		def Kmeans(self,dataset,d):
			#随机初始分配
			def RandomSep(self):
				keys = dataset.keys()
				Len = len(keys)/d
				GraphGroup = []
				for i in range(d):
					subGraphGroup = []
					startIndex = 0+i*Len
					stopIndex = (i+1)*Len-1
					if stopIndex > len(keys):
						stopIndex = len(keys)
					for j in keys[startIndex:stopIndex]:
						subGraphGroup.append(dataset[j])
					GraphGroup.append(subGraphGroup)
				return GraphGroup
			#计算聚类中心
			def CalCentroidView(self,GraphGroup):
				CentroidGroup = []
				for subgroup in GraphGroup:
					centGraph = self.gview([])
					for graph in subgroup:
						centGraph.merge(centGraph,graph)
					CentroidGroup.append(centGraph)
				for index in range(len(CentroidGroup)):
					Centroid = CentroidGroup[index]
					h = len(GraphGroup[index])
					for node in Centroid.nodes:
						Centroid.nodes[node] = math.ceil(float(Centroid.nodes[node])/h)
					for edge in Centroid.edges:
						Centroid.edges[edge] = math.ceil(float(Centroid.edges[edge])/h)

				return CentroidGroup

			#判断前后两次聚类是否有变化
			def CompareMat(self,mat1,mat2,c):
				judgeCount = 0
				threshold = 1*len(mat1)/10
				for index1 in range(len(mat1)):
					tempjudgeCount = 0
					for index2 in range(len(mat1[index1])):
						if not mat1[index1][index2] in mat2[index1]:
							tempjudgeCount += 1
							if tempjudgeCount > len(mat1[index1])/4:
								judgeCount += 1
								break
				#for i in range(len(mat2)):
				#	print 'Group:',i,'	len:',len(mat2[i])
				if judgeCount > threshold:				
					print 'iteration:',c,'	difference:',judgeCount,'/',threshold,'/',len(mat1)
					return False
				elif judgeCount :
					print 'iteration:',c,'	difference:',judgeCount,'/',threshold,'/',len(mat1)
					return True

			#存储聚类最终结果
			def saveKmeans(self,group,centr):
				db = MyZODB('Data.fs')
				dbrootN = db.dbroot
				#temphash = {}

				for index in range(len(centr)):
					dbrootN[index] = {}
					dbrootN[index]['Centroid'] = {}
					dbrootN[index]['Centroid']['nodes'] = {}
					dbrootN[index]['Centroid']['edges'] = {}
					for node in centr[index].nodes:
						dbrootN[index]['Centroid']['nodes'][node] = centr[index].nodes[node]
					for edge in centr[index].edges:
						dbrootN[index]['Centroid']['edges'][edge] = centr[index].edges[edge]
					dbrootN[index]['GraphGroup'] = {}
					for gviewIndex in range(len(group[index])):
						dbrootN[index]['GraphGroup'][gviewIndex] = {}
						dbrootN[index]['GraphGroup'][gviewIndex]['nodes'] = {}
						dbrootN[index]['GraphGroup'][gviewIndex]['edges'] = {}
						for node in group[index][gviewIndex].nodes:
							dbrootN[index]['GraphGroup'][gviewIndex]['nodes'][node] = group[index][gviewIndex].nodes[node]
						for edge in group[index][gviewIndex].edges:
							dbrootN[index]['GraphGroup'][gviewIndex]['edges'][edge] = group[index][gviewIndex].edges[edge]

				#dbrootN = temphash
				#print dbrootN
				transaction.commit()
				db.close()





			#循环聚类
			def iteration(self):
				GraphGroup = RandomSep(self)
				CentroidGroup = CalCentroidView(self,GraphGroup)
				c = 0
				while True:
					c += 1
					if c > 100:
						saveKmeans(self,GraphGroup_new,CentroidGroup)
						return GraphGroup,CentroidGroup
					GraphGroup_new = []
					for i in range(len(GraphGroup)):
						GraphGroup_new.append([])
					for groupIndex in range(len(GraphGroup)):
						subgroup = GraphGroup[groupIndex]
						for gviewIndex in range(len(subgroup)):
							similarity = []
							for Centroid in CentroidGroup:
								simivalue = self.CalSim(subgroup[gviewIndex],Centroid)
								similarity.append(simivalue)
							maxValueIndex = similarity.index(max(similarity))
							GraphGroup_new[maxValueIndex].append(subgroup[gviewIndex])
							#print subgroup[gviewIndex],'	result:',groupIndex,'->',maxValueIndex
					bl = CompareMat(self,GraphGroup,GraphGroup_new,c)
					if bl:
						saveKmeans(self,GraphGroup_new,CentroidGroup)
						return GraphGroup_new,CentroidGroup
					else:
						CentroidGroup = CalCentroidView(self,GraphGroup_new)
						GraphGroup = GraphGroup_new
						continue


			self.GraphGroup,self.CentroidGroup = iteration(self)

		#建立倒排表
		def CreatInvertedList(self):
			'''
			def saveData(self):
				db = MyZODB('Data.fs')
				dbrootN = db.dbroot
				dbrootN['gviews'] = {}
				dbrootN['InvertedList'] = {}
				for g in self.gSetToMusic:
					dbrootN['gviews'][g] = {}
					dbrootN['gviews'][g]['nodes'] = {}
					dbrootN['gviews'][g]['edges'] = {}
					for node in self.gSetToMusic[g].nodes:
						dbrootN['gviews'][g]['nodes'][node] = self.gSetToMusic[g].nodes[node]
					for edge in self.gSetToMusic[g].edges:
						dbrootN['gviews'][g]['edges'][edge] = self.gSetToMusic[g].edges[edge]
				for t in self.InvertedList:
					dbrootN['InvertedList'][t] = self.InvertedList[t]
				transaction.commit()
				db.close()
			'''
			def saveData(self):
				db = MyZODB('Data2.fs')
				dbrootN = db.dbroot
				dbrootN['gviews'] = {}
				dbrootN['InvertedList'] = {}
				for g in self.gSetToMusic:
					dbrootN['gviews'][g] = self.gSetToMusic[g]
				for t in self.InvertedList:
					dbrootN['InvertedList'][t] = self.InvertedList[t]
				transaction.commit()
				db.close()				


			for sl in self.gSetToMusic:
				for node in self.gSetToMusic[sl].nodes:
					if self.InvertedList.has_key(node):
						temphash = self.InvertedList[node]
						temphash[sl] = self.gSetToMusic[sl].nodes[node]
						self.InvertedList[node] = temphash
					else:
						temphash = {}
						temphash[sl] = self.gSetToMusic[sl].nodes[node]
						self.InvertedList[node] = temphash
			saveData(self)



		LoadingData(self,33333)
		GetGraphSet(self)
		#Kmeans(self,self.gSetToMusic,1000)
		CreatInvertedList(self)

	
	#查询
	def search(self,tags,SongNum,MusicSet,InvertedList):
		
		def hashInCommon(hash1,hash2):
			temphash = {}
			if len(hash1) == 0:return hash2
			if len(hash2) == 0:return hash1

			if len(hash1) > len(hash2):
				for i in hash2:
					if hash1.has_key(i):
						temphash[i] = ''
			else:
				for i in hash1:
					if hash2.has_key(i):
						temphash[i] = ''
			return temphash
		def parse(key):
			try:
				tempstr = self.yc[key]
			except:
				return ' ',' ',' '
			s = re.compile(r'<song:(.*?)>').findall(tempstr)
			al = re.compile(r'<album:(.*?)>').findall(tempstr)
			ar = re.compile(r'<artist:(.*?)>').findall(tempstr)
			try:
				tempmat = []
				for i in s:
					tempmat.append(self.uri2name[i])
				song = '/'.join(tempmat)
			except:
				song = ' '
			try:
				#if self.querry in al:
				#	album = self.querry
				#else:
				tempmat = []
				for i in al:
					tempmat.append(self.uri2name[i])
					#album = '/'.join(tempmat)
				album = tempmat[0]
			except:
				album = ' '
			try:
				#if self.querry in ar:
				#	artist = self.querry
				#else:
				tempmat = []
				for i in ar:
					tempmat.append(self.uri2name[i])
				artist = tempmat[0]
			except:
				artist = ' '
			return song,album,artist	
		myhash = {}		
		tag2 = []
		for i in tags:
			i = i.decode('utf-8').encode('gbk')
			
			if InvertedList.has_key(i):
				tag2.append(i)
				myhash = hashInCommon(myhash,InvertedList[i])
				print len(myhash)
		print 'tags:',','.join(tag2)
		Usergview = self.gview([tag2])
		scorehash = {}
		for i in myhash:
			scorehash[i] = self.CalSim(Usergview,MusicSet[i])
		c = 0
		keys = sorted(scorehash.iteritems(),key=lambda scorehash:scorehash[1],reverse=True)
		for i in keys:
			if i[0] == '':continue
			c += 1
			if c > SongNum:break
			#song,album,artist = parse(i[0])
			song = self.id2name[i[0]]
			print c,':',song,i[1]
			#print ','.join([tag+str(MusicSet[i[0]].nodes[tag]) for tag in MusicSet[i[0]].nodes])
			#print '-------------------------------'











t = TGBR()


db = anydbm.open('songlistDB_baidu.db','r')
#t.traning(db)
MusicSet,InvertedList = t.loadingMODB('Data.fs')
#print InvertedList
tags = ['感动','夜晚','怀旧','经典',]
t.search(tags,10,MusicSet,InvertedList)










