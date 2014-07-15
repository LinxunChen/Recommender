from math import sqrt
import time

critics={'Lisa Rose': {'Lady in the Water': 2.5, 'Snakes on a Plane': 3.5,
 'Just My Luck': 3.0, 'Superman Returns': 3.5, 'You, Me and Dupree': 2.5,
 'The Night Listener': 3.0},
'Gene Seymour': {'Lady in the Water': 3.0, 'Snakes on a Plane': 3.5,
 'Just My Luck': 1.5, 'Superman Returns': 5.0, 'The Night Listener': 3.0,
 'You, Me and Dupree': 3.5},
'Michael Phillips': {'Lady in the Water': 2.5, 'Snakes on a Plane': 3.0,
 'Superman Returns': 3.5, 'The Night Listener': 4.0},
'Claudia Puig': {'Snakes on a Plane': 3.5, 'Just My Luck': 3.0,
 'The Night Listener': 4.5, 'Superman Returns': 4.0,
 'You, Me and Dupree': 2.5},
'Mick LaSalle': {'Lady in the Water': 3.0, 'Snakes on a Plane': 4.0,
 'Just My Luck': 2.0, 'Superman Returns': 3.0, 'The Night Listener': 3.0,
 'You, Me and Dupree': 2.0},
'Jack Matthews': {'Lady in the Water': 3.0, 'Snakes on a Plane': 4.0,
 'The Night Listener': 3.0, 'Superman Returns': 5.0, 'You, Me and Dupree': 3.5},
'Toby': {'Snakes on a Plane':4.5,'You, Me and Dupree':1.0,'Superman Returns':4.0},
'CLX':{'Just My Luck': 4.0},'TEST':{'You, Me and Dupree': 3.5}}


def sim_distance(prefs, person1, person2):
    si={}
    for item in prefs[person1]:
        if item in prefs[person2]:
                si[item] = 1
                
    if  len(si) == 0 :
        return 0

    sum_of_quares = sum([pow(prefs[person1][item] - prefs[person2][item] , 2)
         for item in si])        
    return 1 / (1 + sum_of_quares)

def sim_pearson(prefs,p1,p2):
    si={}
    for item in prefs[p1]:
        if item in prefs[p2]: si[item]=1

    n=len(si)
    if n==0: return 0

    sum1=sum([prefs[p1][it] for it in si])
    sum2=sum([prefs[p2][it] for it in si])

    sum1Sq=sum([pow(prefs[p1][it],2) for it in si])
    sum2Sq=sum([pow(prefs[p2][it],2) for it in si])

    pSum=sum([prefs[p1][it]*prefs[p2][it] for it in si])

    num=pSum-(sum1*sum2/n)
    den=sqrt((sum1Sq-pow(sum1,2)/n)*(sum2Sq-pow(sum2,2)/n))
    if den==0: return 0
    r=num/den
    return r

def simDict(prefs, similarity=sim_pearson):
    k=0
    result = {}
    for u in prefs:
        for v in prefs:
            if u == v:
                continue
            if u in result and v in result[u]:
                continue
            result.setdefault(u, {})
            result[u][v] = similarity(prefs,u, v)
            result.setdefault(v, {})
            result[v][u] = result[u][v]
            k = k + 1
            print k
    return result

'''def topMatches(prefs, person, n=5, similarity=sim_pearson):
    scores = [(similarity(prefs, person, other), other)
              for other in prefs if other != person]
    scores.sort()
    scores.reverse()
    neighbor = [i[1] for i in scores][0:n]
    return neighbor'''

def topMatches(sim, person, n=5):
    sim_sort = sorted(sim[person].items(), key=lambda d:d[1], reverse=True)
    neighbor = [sim_sort[i][0] for i in range(len(sim_sort))]
    return neighbor[0:n]

def getRecommendations(prefs, person, n=20, similarity=sim_pearson):
    totals={}
    simSums={}
    start = time.clock()
    simD = simDict(prefs,similarity=sim_pearson)
    end1 = time.clock()
    neighbor = topMatches(simD, person, n)
    end2 = time.clock()
    print end1-start
    print end2-end1
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

# def loadMovieLens(path=r'E:\USTC\2.Recommendation System\ml-10m\ml-10M100K'):
#     movies = {}
#     for line in open(path+r'\movies.dat'):
#         (id, title) = line.split('::')[0:2]
#         movies[id] = title
#
#     prefs = {}
#     for line in open(path+r'\ratings.dat'):
#         (user, movieid, rating) = line.split('::')[0:3]
#         prefs.setdefault(user,{})
#         prefs[user][movies[movieid]] = float(rating)
#
#     return prefs

def loadMovieLens(path=r'E:\USTC\2.Recommendation System\ml-100k'):
    movies = {}
    for line in open(path+r'\u.item'):
        (id, title) = line.split('|')[0:2]
        movies[id] = title

    prefs = {}
    for line in open(path+r'\u.data'):
        (user, movieid, rating) = line.split('\t')[0:3]
        prefs.setdefault(user,{})
        prefs[user][movies[movieid]] = float(rating)

    return prefs

if __name__ == "__main__":
    prefs = loadMovieLens()
    print getRecommendations(prefs,'87')[0:30]
    # print getRecommendations(critics,'Toby')