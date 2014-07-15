__author__ = 'clx'
from recommendations_beta1 import *
import time

start = time.clock()

prefs = loadMovieLens()
end1 = time.clock()

print getRecommendations(prefs,'87')[0:30]

end2 = time.clock()
print end1-start
print end2-end1
# print topMatches(critics,'Toby',n=3)
# print getRecommendations(critics, 'Toby')