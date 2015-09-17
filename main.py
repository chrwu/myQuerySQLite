import myQuerySQLite as mq
import random as rd


def genData(n=20, nSubjects=3):
    data = []
    for i in range(n):
        nSC = ''
        l = str(hex(4))+str(hex(0))+str(hex(0))+str(hex(0))
        for j in range(nSubjects):
            jj = j+1
            sc = rd.randint(0,100)
            t = str(hex(jj))
            v = str(hex(sc))  
            nSC += t+l+v
        ii = i+1
        #print nSC
        #print nSC.replace("Ox", "\\x")
        x = (ii, 'a'+str(ii), rd.choice('MF'), mq.sqlite3.Binary(nSC))
        data.append(x)
    return data

if __name__ == '__main__':
    data = genData()

    mqsqlite = mq.myQuerySQLite('chrwu.db','classA')
    mqsqlite.insertData(data)
    mqsqlite.displayStudentNames()
    mqsqlite.getNoOfStudent()
    mqsqlite.displayHighestScore()
    mqsqlite.displayRankingList()
    mqsqlite.displayByScore()

    mqsqlite.closeDBConnection()
