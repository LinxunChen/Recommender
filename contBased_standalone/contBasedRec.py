__author__ = 'clx'
#coding:utf-8
from math import sqrt

#资料内容信息字典，subject为科目，grade为年级，volume为册数（上/下），publisher为出版社信息
content = {'book1': {'subject': 5, 'grade': 3, 'volume': 2, 'publisher': 8},
           'book2': {'subject': 7, 'grade': 4, 'volume': 1, 'publisher': 5},
           'book3': {'subject': 3, 'grade': 1, 'volume': 2, 'publisher': 9},
           'book4': {'subject': 3, 'grade': 1, 'volume': 1, 'publisher': 3}}

def simCosine(content, item1, item2, weight1=1, weight2=0.8, weight3=0.6, weight4=0.4):
    """
    针对资料的内容信息进行相似度的计算，计算要考虑各类目信息的权重，基于余弦相似度进行计算
    计算公式（a、b、wei分别为资料1内容向量、资料2内容向量、权重向量）：
    {1-[(a-b)*wei]^2/(a-b)^2}*{[(a*wei)*(b*wei)]/[(a*wei)^2*(b*wei)^2]}
    :param content:资料内容信息字典
    :param item1:资料1
    :param item2:资料2
    :param weight1:科目信息的权重
    :param weight2:年级信息的权重
    :param weight3:册数信息的权重
    :param weight4:出版社信息的权重
    :return:资料1与资料2的相似度
    """
    weightDict = {'subject':weight1, 'grade': weight2, 'volume': weight3, 'publisher': weight4}
    si = {}
    for info in content[item1]:
        if info in content[item2]:
            si[info] = 1
    if len(si) == 0:
        return 0
    r1 = []
    r2 = []
    weight = []

    for info in si:
        r1.append(content[item1][info] * weightDict[info])
        r2.append(content[item2][info] * weightDict[info])
        weight.append(weightDict[info])
    fenzi = sum([r1[i] * r2[i] for i in range(len(r1))])
    temp1 = sum([r1[i] * r1[i] for i in range(len(r1))])
    temp2 = sum([r2[i] * r2[i] for i in range(len(r2))])
    fenmu = sqrt(temp1 * temp2)
    r = fenzi / fenmu

    diff = [r1[i] - r2[i] for i in range(len(r1))]
    diffWithWei = [diff[i] * weight[i] for i in range(len(diff))]
    fenziWei = sum([pow(diffWithWei[i], 2) for i in range(len(diff))])
    fenmuWei = sum([pow(diff[i], 2) for i in range(len(diff))])
    if fenmuWei == 0:
        wei = 1
    else:
        wei = 1 - (fenziWei / fenmuWei)
    if wei == 0:
        wei = 1

    result = r * wei
    return result

if __name__ == "__main__":
    print simCosine(content, 'book3', 'book4')
