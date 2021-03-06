__author__ = 'clx'
#coding:utf-8
from math import sqrt
# import time
import random
# import os
# import cPickle as p
import numpy

COUNTER = 0 #标记，COUNTER为1表示相似度矩阵已经计算完成，接下来不用再计算
simD = {} #相似度矩阵（字典形式）

# 此部分训练集和测试集为验证程序正确性使用
# critics={'Lisa Rose': {'Lady in the Water': 2.5, 'Snakes on a Plane': 3.5,
#  'Just My Luck': 3.0, 'Superman Returns': 3.5, 'You, Me and Dupree': 2.5,
#  'The Night Listener': 3.0},
# 'Gene Seymour': {'Lady in the Water': 3.0, 'Snakes on a Plane': 3.5,
#  'Just My Luck': 1.5, 'Superman Returns': 5.0, 'The Night Listener': 3.0,
#  'You, Me and Dupree': 3.5},
# 'Michael Phillips': {'Lady in the Water': 2.5, 'Snakes on a Plane': 3.0,
#  'Superman Returns': 3.5, 'The Night Listener': 4.0},
# 'Claudia Puig': {'Snakes on a Plane': 3.5, 'Just My Luck': 3.0,
#  'The Night Listener': 4.5, 'Superman Returns': 4.0,
#  'You, Me and Dupree': 2.5},
# 'Mick LaSalle': {'Lady in the Water': 3.0, 'Snakes on a Plane': 4.0,
#  'Just My Luck': 2.0, 'Superman Returns': 3.0, 'The Night Listener': 3.0,
#  'You, Me and Dupree': 2.0},
# 'Jack Matthews': {'Lady in the Water': 3.0, 'Snakes on a Plane': 4.0,
#  'The Night Listener': 3.0, 'Superman Returns': 5.0, 'You, Me and Dupree': 3.5},
# 'Toby': {'Snakes on a Plane':4.5,'You, Me and Dupree':1.0,'Superman Returns':4.0}}
#
# criticsTest = {'Toby':{'The Night Listener':2, 'Lady in the Water':3,  'Just My Luck': 4}, 'Claudia Puig':{'Lady in the Water':3}}


def simDistance(prefs, person1, person2):
    '''
    采用欧几里得距离来计算相似度
    :param prefs:用户的评分字典，形式为{user:{item1:pref, item2:pref...},...}
    :param person1:用户1
    :param person2:用户2
    :return:用户1与用户2的相似度
    '''
    si={}
    for item in prefs[person1]:
        if item in prefs[person2]:
                si[item] = 1

    if  len(si) == 0 :
        return 0

    sum_of_quares = sum([pow(prefs[person1][item] - prefs[person2][item] , 2)
         for item in si])
    return 1 / (1 + sqrt(sum_of_quares))

def simPearson(prefs, p1, p2):
    '''
    采用皮尔逊相关系数来计算相似度
    :param prefs:用户评分字典
    :param p1:用户1
    :param p2:用户2
    :return:用户1与用户2的相似度
    '''
    si={}
    for item in prefs[p1]:
        if item in prefs[p2]:
            si[item]=1

    n=len(si)
    if n==0:
        return 0

    sum1=sum([prefs[p1][it] for it in si])
    sum2=sum([prefs[p2][it] for it in si])

    sum1Sq=sum([pow(prefs[p1][it],2) for it in si])
    sum2Sq=sum([pow(prefs[p2][it],2) for it in si])

    pSum=sum([prefs[p1][it]*prefs[p2][it] for it in si])

    num=pSum-(sum1*sum2/n)
    den=sqrt((sum1Sq-pow(sum1,2)/n)*(sum2Sq-pow(sum2,2)/n))
    if den==0:
        return 0
    r=num/den
    return r

def simCosine(prefs, p1, p2):
    '''
    采用余弦相似度来表征相似度
    :param prefs:用户评分字典
    :param p1:用户1
    :param p2:用户2
    :return:用户1与用户2的相似度
    '''
    si={}
    for item in prefs[p1]:
        if item in prefs[p2]:
            si[item]=1
    if len(si) == 0:
        return 0
    r1 = []
    r2 = []
    for item in si:
        r1.append(prefs[p1][item])
        r2.append(prefs[p2][item])
    fenzi = sum([r1[i] * r2[i] for i in range(len(r1))])
    temp1 = sum([r1[i] * r1[i] for i in range(len(r1))])
    temp2 = sum([r2[i] * r2[i] for i in range(len(r2))])
    fenmu = sqrt(temp1 * temp2)
    r = fenzi / fenmu
    return r

# def simCosineImp(prefs, p1, p2):
#     '''
#     改进的余弦相似度，因transformPrefs改动而废弃
#     :param prefs:用户评分字典
#     :param p1:用户1
#     :param p2:用户2
#     :return:用户1与用户2的相似度
#     '''
#     transPrefs = transformPrefs(prefs)
#     rMean = {}
#     for item in transPrefs:
#         rMean[item] = numpy.mean([transPrefs[item][key] for key in transPrefs[item]])
#     si={}
#     for item in prefs[p1]:
#         if item in prefs[p2]:
#             si[item]=1
#     r1 = []
#     r2 = []
#     for item in si:
#         r1.append(prefs[p1][item] - rMean[item])
#         r2.append(prefs[p2][item] - rMean[item])
#     fenzi = sum([r1[i] * r2[i] for i in range(len(r1))])
#     temp1 = sum([r1[i] * r1[i] for i in range(len(r1))])
#     temp2 = sum([r2[i] * r2[i] for i in range(len(r2))])
#     fenmu = sqrt(temp1 * temp2)
#     r = fenzi / fenmu
#     return r

def transformPrefs(prefs):
    '''
    对用户的评分矩阵进行倒排，变成{item:{user1:pref, user2:pref...},...}的形式
    :param prefs:用户评分字典
    :return:倒排的评分字典
    '''
    result = {}
    for user in prefs:
        for item in prefs[user]:
            result.setdefault(item, []).append(user)
    return result

def simDict(prefs, item_users, similarity):
    '''
    将两两用户间的相似度统一起来，形成用户相似度矩阵（字典）
    :param prefs: 用户评分字典
    :param item_users: 倒排的评分字典
    :param similarity: 设定采用哪种相似度计算方法，可选simCosine、simDistance、simPearson
    :return:相似度矩阵（字典的形式）
    '''
    global simD
    global COUNTER
    COUNTER = COUNTER + 1
    result = {}
    for item in item_users:
        for u in item_users[item]:
            for v in item_users[item]:
                if u == v:
                    continue
                if u in result and v in result[u]:
                    continue
                result.setdefault(u, {})
                result[u][v] = similarity(prefs,u, v)
                result.setdefault(v, {})
                result[v][u] = result[u][v]
    # f = file('./simD.f', 'w')
    # p.dump(result, f)
    # f.close
    simD = result
    # print 'sim dict is ok'
    # return result

def topMatches(sim, person, n):
    '''
    输出与该用户相似度最高的前n个用户
    :param sim: 相似度矩阵
    :param person: 用户person
    :param n: 输出邻居的数量
    :return:输出一个列表，用户按相似度从高到低排列
    '''
    sim_sort = sorted(sim[person].items(), key=lambda d:d[1], reverse=True)
    neighbor = [sim_sort[i][0] for i in range(len(sim_sort))]
    return neighbor[0:n]

def getRecommendations(prefs,transPrefs, person, n, similarity=simPearson):
    '''
    给用户person进行推荐，推荐列表长度为n
    :param prefs:用户评分字典
    :param transPrefs:倒排的评分字典
    :param person:用户person
    :param n:邻居的数量
    :param similarity:选择相似度计算方法
    :return:已排序的推荐列表
    '''
    global simD
    global COUNTER
    totals={}
    simSums={}
    # start = time.clock()
    if COUNTER == 0:
        simDict(prefs, transPrefs, similarity)
    # else:
    #     f = file('./simD.f')
    #     simD = p.load(f)
    neighbor = topMatches(simD, person, n)
    for other in neighbor:
        if other == person: continue
        sim = simD[other][person]

        if sim <= 0: continue
        for item in prefs[other]:
            if item not in prefs[person] or prefs[person][item]==0:
                totals.setdefault(item,0)
                totals[item] += sim * prefs[other][item]

                simSums.setdefault(item,0)
                simSums[item] += sim

    rankings = [(round(totals[item]/simSums[item], 5), item) for item in totals]

    rankings.sort()
    rankings.reverse()
    return rankings

def loadData(dataSegNum, expOrder, path=r'E:\USTC\2.Recommendation System\ml-100k'):
    '''
    读取单个数据集文件，形成字典形式的训练集与测试集；u.item形成电影名与电影id的映射，u.data为用户对物品的评分数据集；
    将u.dada数据集分为dataSegNum份，第expOrder份设为测试集，其他份合并设为训练集
    :param dataSegNum: 数据集的分割份数
    :param expOrder: 第expOrder份设为测试集
    :param path: 数据集的路径
    :return:字典形式的训练集与测试集
    '''
    global COUNTER
    COUNTER = 0
    train = []
    test = []
    movies = {}
    prefsTrain = {}
    prefsTest = {}
    for line in open(path+r'\u.item'):
        (id, title) = line.split('|')[0:2]
        movies[id] = title
    random.seed(1)
    for line in open(path+r'\u.data'):
        (user, movieid, rating) = line.split('\t')[0:3]
        if random.randint(0, dataSegNum) == expOrder:
            test.append([user,movieid,rating])
        else:
            train.append([user,movieid,rating])
    for user,movieid,rating in train:
        prefsTrain.setdefault(user,{})
        prefsTrain[user][movies[movieid]] = float(rating)
    for user,movieid,rating in test:
        prefsTest.setdefault(user,{})
        prefsTest[user][movies[movieid]] = float(rating)
    return [prefsTrain, prefsTest]

def loadDataFromTwo( path=r'E:\USTC\2.Recommendation System\ml-100k' ):
    '''
    分别读取训练集与测试集文件，形成字典形式的训练集与测试集；u.item形成电影名与电影id的映射，ua.base为训练集文件，ua.test为测试集文件；
    :param path:数据集的路径
    :return:字典形式的训练集与测试集
    '''
    global COUNTER
    COUNTER = 0
    train = []
    test = []
    movies = {}
    prefsTrain = {}
    prefsTest = {}
    for line in open(path+r'\u.item'):
        (id, title) = line.split('|')[0:2]
        movies[id] = title

    for line in open(path+r'\ua.base'):
        (user, movieid, rating) = line.split('\t')[0:3]
        train.append([user,movieid,rating])
    for line in open(path+r'\ua.test'):
        (user, movieid, rating) = line.split('\t')[0:3]
        test.append([user,movieid,rating])

    for user,movieid,rating in train:
        prefsTrain.setdefault(user,{})
        prefsTrain[user][movies[movieid]] = float(rating)
    for user,movieid,rating in test:
        prefsTest.setdefault(user,{})
        prefsTest[user][movies[movieid]] = float(rating)
    return [prefsTrain, prefsTest]

def eachEval(prefs,transPrefs, person, test, n, topN, similarity=simPearson):
    '''
    对每个用户person进行评价计算，为计算RMSE，Precision，Recall做准备
    :param prefs:用户评分字典（训练集）
    :param transPrefs:倒排的评分字典
    :param person:用户person
    :param test:测试集字典
    :param n:邻居的数量
    :param topN:选取推荐列表前topN个项目
    :param similarity:选择相似度计算方法
    :return:RMSE计算需要的预测评分与真实评分的平方和以及参与计算的数目；Precision和Recall计算需要的列表元素数量
    '''
    trainTemp = getRecommendations(prefs,transPrefs, person, n, similarity)
    train = {}
    sum = 0
    count = 0
    for i in range(len(trainTemp)):
        train[trainTemp[i][1]] = trainTemp[i][0]
    for item in test[person]:
        if item in train:
            sum += pow((test[person][item] - train[item]), 2)
            count += 1

    ru = []
    tu = []
    hit = 0
    prefsRating = []
    for item in prefs[person]:
        prefsRating.append(prefs[person][item])
    mean = numpy.mean(prefsRating)
    std = numpy.std(prefsRating)
    thresh = mean - std
    for rating, item in trainTemp:
        ru.append(item)
    ru = ru[0:topN]
    for item in test[person]:
        rating = test[person][item]
        if rating >= thresh:
            tu.append(item)
    for i in ru:
        if i in tu:
            hit += 1
    nPrecision = len(ru)
    nRecall = len(tu)
    return [sum, count, hit, nPrecision, nRecall]


def evaluate(prefs,transPrefs,test, n, topN=20, similarity=simPearson):
    '''
    计算RMSE，Precision，Recall，F1值
    :param prefs: 用户评分字典（训练集）
    :param transPrefs: 倒排的评分字典
    :param test: 测试集字典
    :param n: 邻居的数量
    :param topN: 选取推荐列表前topN个项目
    :param similarity: 选择相似度计算方法
    :return:RMSE，F1值，Precision，Recall
    '''
    sum = 0
    count = 0
    hit = 0
    nPrecision = 0
    nRecall = 0
    for user in test:
        if user in prefs:
            each = eachEval(prefs,transPrefs, user, test, n, topN, similarity)
            sum += each[0]
            count += each[1]
            hit += each[2]
            nPrecision += each[3]
            nRecall += each[4]
    RMSE = sqrt(sum / count)
    if nRecall == 0:
        recall = 0
    else:
        recall = hit / (1.0 * nRecall)
    if nPrecision == 0:
        precision = 0
    else:
        precision = hit / (1.0 * nPrecision)
    if precision==0 or recall==0:
        F1 = 0
    else:
        F1 = 2 * recall *precision / (recall + precision)
    return [RMSE, F1, precision, recall]

if __name__ == "__main__":
    # 下面是完整评测功能（训练集：测试集=(dataSegNum-1):1，为了防止过拟合做dataSegNum次实验，取均值）：
    dataSegNum = 8
    RMSE = 0
    F1 = 0
    recall = 0
    precision = 0
    for expOrder in range(dataSegNum):
        (prefsTrain, prefsTest) = loadData(dataSegNum, expOrder)
        transPrefs = transformPrefs(prefsTrain)
        (RMSEEach, F1Each, precisionEach, recallEach) = evaluate(prefsTrain,transPrefs,prefsTest, 320, 10, simPearson)
        RMSE += RMSEEach
        F1 += F1Each
        precision += precisionEach
        recall += recallEach
    print [RMSE/dataSegNum, F1/dataSegNum, precision/dataSegNum, recall/dataSegNum]

    # # 下面是单次评测功能，不做多次实验：
    # # dataSegNum = 8
    # # expOrder = 7
    # (prefsTrain, prefsTest) = loadDataFromTwo()
    # transPrefs = transformPrefs(prefsTrain)
    # print evaluate(prefsTrain,transPrefs,prefsTest, 1000, 20, simDistance)

    # # 下面是为单个用户进行推荐的功能：
    # dataSegNum = 8
    # expOrder = 7
    # (prefsTrain, prefsTest) = loadData(dataSegNum, expOrder)
    # transPrefs = transformPrefs(prefsTrain)
    # print getRecommendations(prefsTrain,transPrefs, '87', 320, similarity=simCosine)[0:10]

    # '''下面是推荐正确性验证功能：'''
    # prefsTrain = critics
    # prefsTest = criticsTest
    # transPrefs = transformPrefs(prefsTrain)
    # print getRecommendations(prefsTrain,transPrefs, 'Toby', 320, similarity=simDistance)[0:10]
    # print evaluate(prefsTrain,transPrefs,prefsTest, 320, 10, simDistance)
