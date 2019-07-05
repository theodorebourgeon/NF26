# -*- coding: utf-8 -*-
"""
Created on Fri jun 21 23:09:15 2019
K-means clustering en python
@author: haoxuandong
"""

import numpy as np

class KMeansClassifier():
    "this is a k-means classifier"
    
    def __init__(self, k=3, initCent='random', max_iter=500 ):
        
        self._k = k
        self._initCent = initCent
        self._max_iter = max_iter
        self._clusterAssment = None
        self._labels = None
        self._sse = None

    def _calEDist(self, arrA, arrB):
        """
        fonction: calcule distance euclidienne
        entree: 2 list de dim(1)
        """
        return np.math.sqrt(sum(np.power(arrA-arrB, 2)))
    
    def _calMDist(self, arrA, arrB):
        """
        fonction: calcule distance Manhattan
        entree: 2 list de dim(1)
        """
        return sum(np.abs(arrA-arrB))


    def _randCent(self, data_X, k):
        """
        fonction: choisi aleatoirement k centres
        out：centroids matrice de n*p
        """
        n = data_X.shape[1] #get dim() de variable
        centroids = np.empty((k,n))  #intialisation de matrice de centres
        for j in range(n):
            minJ = min(data_X[:, j])
            rangeJ  = float(max(data_X[:, j] - minJ))
            #(nested list)
            centroids[:, j] = (minJ + rangeJ * np.random.rand(k, 1)).flatten()
        return centroids 
    
    def fit(self, data_X):
        """
        finction: precessus d'aprentissage
        data_X : matrice n*p
        """
        if not isinstance(data_X, np.ndarray) or \
               isinstance(data_X, np.matrixlib.defmatrix.matrix):
            try:
                data_X = np.asarray(data_X)
            except:
                raise TypeError("numpy.ndarray resuired for data_X")
                
        m = data_X.shape[0]  #prends nb individus
        # matrice de m carre, 1er col est l'index de cluster
        # 第二列存储该点与所属族的质心的平方误差
        # 2em col est sse
        self._clusterAssment = np.zeros((m,2)) 
        
        if self._initCent == 'random':
            self._centroids = self._randCent(data_X, self._k)
            
        clusterChanged = True
        for _ in range(self._max_iter):
            clusterChanged = False
            for i in range(m):   #main pluster
                minDist = np.inf
                minIndex = -1    # index de centre
                for j in range(self._k): # cherche centre le plus proche
                    arrA = self._centroids[j,:]
                    arrB = data_X[i,:]
                    distJI = self._calEDist(arrA, arrB) #calcule sse (*** a voir la distance manhattan *** )
                    if distJI < minDist:
                        minDist = distJI
                        minIndex = j
                if self._clusterAssment[i, 0] != minIndex or self._clusterAssment[i, 1] > minDist**2:
                    clusterChanged = True
                    self._clusterAssment[i,:] = minIndex, minDist**2
            if not clusterChanged: #test de convergence
                break
            for i in range(self._k):#mise a jour du centre des clusters
                index_all = self._clusterAssment[:,0] #取出样本所属簇的索引值
                value = np.nonzero(index_all==i) #取出所有属于第i个簇的索引值
                ptsInClust = data_X[value[0]]    #取出属于第i个簇的所有样本点
                self._centroids[i,:] = np.mean(ptsInClust, axis=0) #计算均值
        
        self._labels = self._clusterAssment[:,0]
        self._sse = sum(self._clusterAssment[:,1])
    
    def predict(self, X):
        '''
        fonction: cluster de nouveau individu
        '''
        if not isinstance(X,np.ndarray):
            try:
                X = np.asarray(X)
            except:
                raise TypeError("numpy.ndarray required for X")
        
        m = X.shape[0]
        preds = np.empty((m,))
        for i in range(m): #attachement des individus au cluster dont le centre est le plus proche
            minDist = np.inf
            for j in range(self._k):
                distJI = self._calEDist(self._centroids[j,:], X[i,:])
                if distJI < minDist:
                    minDist = distJI
                    preds[i] = j
        return preds
