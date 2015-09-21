import operator
import sqlite3
import random as rd
from collections import defaultdict

class myQuerySQLite:
    def __init__(self, dbname, dbtable):
        self.nSubject = 3
        self.ntlv = 6
        self.subjectMap = {0:'all',1:'chinese',2:'english',3:'math'}

        # db setting
        self.dbname = dbname
        self.dbtable = dbtable
        self.conn = sqlite3.connect(dbname)
        self.cursor = self.conn.cursor()
        self.__creatTable(self.dbtable)
        self.NoOfStudent = self.getNoOfStudent()
    
    def closeDBConnection(self):
        self.cursor.close()
        self.conn.close()

    def __creatTable(self, tablename='classA'):
        sql = '''CREATE TABLE if not exists %s (sid INTERGER, name TEXT, gender TEXT, score BLOB)''' % tablename
        try:
            result = self.cursor.execute(sql)
        except Exception as e:
            print 'Create table failed!'
        return result
    
    def genData(self, nData=20, nSubjects=3):
        data = []
        for i in xrange(nData):
            nScore = ''
            length = str(hex(4))+str(hex(0))+str(hex(0))+str(hex(0))
            for j in xrange(nSubjects):
                score = rd.randint(0,100)
                tag = str(hex(j+1))
                value = str(hex(score))
                nScore += tag+length+value
            data.append((i+1, 'a'+str(i+1), rd.choice('MF'), sqlite3.Binary(nScore)))
        return data

    def insertData(self, data, flag=False):
        if flag:
            insertdata = []
            for i in xrange(len(data)):
                sid = data[i][0]+self.NoOfStudent
                name = data[i][1][0]+str(int(data[i][1][1:])+self.NoOfStudent)
                insertdata.append((sid,name,data[i][2],data[i][3]))
            try:
                self.cursor.executemany('INSERT INTO classA VALUES (?, ?, ?, ?)', insertdata)
                self.conn.commit()
            except:
                print 'Insert data failed!'

    # Display students' name by gender (defalut:F&M)
    def displayStudentNamesByGender(self, sex = 'all'):
        students = self.getStudentNamesByGender(sex)
        print "There are %s stuednts" % len(students) + " with gender %s :" % sex
        for student in self.getStudentNamesByGender(sex):
            print "Student's name: %s" % student[0]
        
    def getStudentNamesByGender(self, sex='all'):
        sql = '''SELECT name, gender FROM classA WHERE gender %s'''
        if sex == 'Female':
            sql = sql % "= \'F\'"
        elif sex == 'Male':
            sql = sql % "= \'M\'"
        else:
            sql = sql % "IS NOT NULL"
        
        try:
            result = self.cursor.execute(sql)
        except:
            print "Query Students' name failed"
        return [student for student in result]

    def displayNoOfStudent(self):
        print "There are %s students in classA" % self.NoOfStudent

    # Get #of students
    def getNoOfStudent(self):
        sql = '''SELECT COUNT(DISTINCT sid) FROM classA WHERE sid IS NOT NULL'''
        try:
            result = self.cursor.execute(sql)
            no = [x[0] for x in result][0]
        except:
            print "getNoOfStudent failed!"
        return no

    # Get Score data 
    def __getScoreData(self):
        sql = '''SELECT name,score FROM classA'''
        try:
            data = self.cursor.execute(sql)
        except:
            print "__getScoreData failed!"

        score = [{}]*self.nSubject
        for x in data:
            name = x[0]
            scData = str(x[1]).split("0x")[1:]

            for i in xrange(self.nSubject):
                tag = int(scData[self.ntlv*i],16)
                length = int(scData[self.ntlv*i+1],16)
                if length == 4:
                    value = int(scData[self.ntlv*i+self.ntlv-1],16)
                else:
                    value = 0
                category = tag-1
                score[category][name] = value
        return score
    
    def __getCategory(self, subject):
        subject = subject.lower()
        category = []
        if subject not in self.subjectMap.values():
            print "The input subject %s didn't exist!" % subject
        else:
            if subject == 'chinese':
                category.append(1)
            elif subject == 'english':
                category.append(2)
            elif subject == 'math' :
                category.append(3)
            else: # all
                category = [i for i in xrange(4) if i > 0]
        return category
    
    # Display the highest Socre People by subject
    def displayHighestScore(self, subject='all'):
        highestscore = self.getHighestScore(subject)
        msg = "The student %s has the highest score %s "
        
        if subject == 'all':
            for i in xrange(1,len(self.subjectMap)):
                print msg % highestscore[i-1] + " in suject %s" % self.subjectMap[i]
        else:
            print msg % highestscore[0] + " in suject %s" % subject
    
    def getHighestScore(self, subject='all'):
        category = self.__getCategory(subject)
        score = self.__getScoreData()
        highestscore = [sorted(score[i-1].items(), key=operator.itemgetter(1))[-1] for i in category]
        return highestscore

    # Display Ranking list
    def displayRankingList(self):
        data = self.getRankingList()[::-1]
        for i in xrange(self.NoOfStudent):
            print "rank:",i+1," name:%s total score:%s" % data[i]

    def getRankingList(self):
        score = self.__getScoreData()
        sumScore={}
        sumScore = defaultdict(lambda: 0, sumScore)

        for i in xrange(self.nSubject):
            for k in score[i].keys():
                sumScore[k] += score[i][k]
        rank = sorted(sumScore.items(), key=operator.itemgetter(1))
        return rank

    # Display Students whose score are above score (default:60)
    def displayStudentAboveScore(self, subject='all', abscore=60):
        data = self.getStudentAboveScore(subject, abscore)
        category = self.__getCategory(subject)
        if len(category) == 1:
            print "Score >= %s" % abscore,"of Subject:%s" % self.subjectMap[category[0]]
            for score in data[0]:
                print "Student: %s score: %s" % score 
        else:
            for i in category:
                print "Score >= %s" % abscore,"of Subject:%s" % self.subjectMap[i]
                for score in data[i-1]:
                    print "Student: %s score: %s" % score 
                print "\n"
            
    def getStudentAboveScore(self, subject='all', abscore=60):
        data = self.__getScoreData()
        category = self.__getCategory(subject)      
        return [[score for score in data[i-1].items() if score[1] >= abscore] for i in category]        