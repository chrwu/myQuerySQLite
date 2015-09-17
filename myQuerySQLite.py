import operator
import sqlite3
from collections import defaultdict

class myQuerySQLite:
    def __init__(self, dbname, dbtable):
        self.dbname = dbname
        self.dbtable = dbtable
        self.nSubject = 3
        self.ntlv = 6
        self.subjectMap = {'1':'chinese','2':'english','3':'math'}

        self.conn = sqlite3.connect(dbname)
        self.c = self.conn.cursor()
        self.c.execute('''CREATE TABLE if not exists classA (sid INTERGER, name TEXT, gender TEXT, score BLOB)''')   
        
        
    def insertData(self, data, flag=False):
        if flag:
            no = self.getNoOfStudent()
            insertdata = []
            for i in range(len(data)):
                x1 = data[i][0]+no
                x2 = data[i][1][0]+str(int(data[i][1][1:])+no) 
                insertdata.append((x1,x2,data[i][2],data[i][3]))
        #print insertdata
        self.c.executemany('INSERT INTO classA VALUES (?, ?, ?, ?)', insertdata)
        self.conn.commit()

    def closeDBConnection(self):
        self.c.close()
        self.conn.close()
        
    # Display students' name by gender (defalut:F&M)    
    def displayStudentNames(self, sex='all'): 
        tmp = '''SELECT DISTINCT * FROM classA WHERE gender %s'''
        if sex == 'F':
            sql = tmp % "= F"
        elif sex == 'M':
            sql = tmp % "= M"
        else:
            sql = tmp % "IS NOT NULL"
        self.c.execute(sql)

    # Get #of students
    def getNoOfStudent(self):
        sql = '''SELECT COUNT(DISTINCT sid) FROM classA WHERE sid IS NOT NULL'''
        tmp = self.c.execute(sql)
        
        no = [x[0] for x in tmp][0]
        print "# of students:", no
        return no
    
        
        
    # Get Score data with name
    def getScoreData(self):
        sql = '''SELECT name,score FROM classA'''
        data = self.c.execute(sql)
        
        sc = [{}]*self.nSubject
        for x in data:
            name = x[0]
            scData = str(x[1]).split("0x")[1:]
            
            for i in range(self.nSubject):
                tag = int(scData[self.ntlv*i],16)
                length = int(scData[self.ntlv*i+1],16)
                if length == 4: 
                    value = int(scData[self.ntlv*i+self.ntlv-1],16)
                else: ## ???
                    value = 0
                
                category = tag-1
                sc[category][name] = value
                #print tag, length, value
        return sc
    
    # Get the highest Socre People by subject  
    def displayHighestScore(self):
        sc = self.getScoreData()
        for i in range(len(sc)):
            highestsc = sorted(sc[i].items(), key=operator.itemgetter(1))[-1]
            msg = "The student:%s have the highest score:%s of subject:" % highestsc
            msg += self.subjectMap[str(i+1)]
            print msg,"\n"
            
    # Display Ranking list    
    def displayRankingList(self):
        sc = self.getScoreData()
        sumSc={}
        sumSc = defaultdict(lambda: 0, sumSc)
        
        for i in range(self.nSubject):
            for k in sc[i].keys():
                sumSc[k] += sc[i][k]
        rank = sorted(sumSc.items(), key=operator.itemgetter(1))
        for i in range(len(rank)):
            print "rank:",i+1," name:%s total score:%s" % rank[i]
            
            
    # Display Students whose score are above sc (default:60)     
    def displayByScore(self, subject='all', absc=60):
        sc = self.getScoreData()
        
        for i in range(self.nSubject):
            print "Score above:%s (contain)" % absc," of Subject:%s" % self.subjectMap[str(i+1)], "\n"
            for k,v in sc[i].iteritems():
                if v >= 60:
                    print "name:%s score:%s" % (k,v)
