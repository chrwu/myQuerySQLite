import operator
import sqlite3
import random as rd
from collections import defaultdict


class MyQuerySQLite:
    def __init__(self, db_name, db_table):
        self.n_subject = 3
        self.n_tlv = 6
        self.subject_map = {0: 'all', 1: 'chinese', 2: 'english', 3: 'math'}

        # db setting
        self.db_name = db_name
        self.db_table = db_table
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()
        self.__create_table(self.db_table)
        self.no_of_student = self.get_no_of_student()
    
    def close_db_connection(self):
        self.cursor.close()
        self.conn.close()

    def __create_table(self, table_name='classA'):
        sql = '''CREATE TABLE if not exists %s (sid INTERGER, name TEXT, gender TEXT, score BLOB)''' % table_name
        try:
            result = self.cursor.execute(sql)
            return result
        except:
            print "create new table failed!"
    
    def generate_data(self, n_data=20, n_subjects=3):
        data = []
        for i in xrange(n_data):
            n_score = ''
            length = str(hex(4))+str(hex(0))+str(hex(0))+str(hex(0))
            for j in xrange(n_subjects):
                score = rd.randint(0, 100)
                tag = str(hex(j+1))
                value = str(hex(score))
                n_score += tag+length+value
            data.append((i+1, 'a'+str(i+1), rd.choice('MF'), sqlite3.Binary(n_score)))
        return data

    def insert_data(self, data, flag=False):
        if flag:
            inserted_data = []
            for i in xrange(len(data)):
                sid = data[i][0]+self.no_of_student
                name = data[i][1][0]+str(int(data[i][1][1:])+self.no_of_student)
                inserted_data.append((sid, name, data[i][2], data[i][3]))
            try:
                self.cursor.executemany('INSERT INTO classA VALUES (?, ?, ?, ?)', inserted_data)
                self.conn.commit()
            except:
                print 'Insert data failed!'

    # Display students' name by gender (defalut:F&M)
    def display_student_names_by_gender(self, sex='all'):
        students = self.get_student_names_by_gender(sex)
        print "There are %s stuednts" % len(students) + " with gender %s :" % sex
        for student in self.get_student_names_by_gender(sex):
            print "Student's name: %s" % student[0]
        
    def get_student_names_by_gender(self, sex='all'):
        sql = '''SELECT name, gender FROM classA WHERE gender '''
        if sex == 'Female':
            sql += "= \'F\'"
        elif sex == 'Male':
            sql += "= \'M\'"
        else:
            sql += "IS NOT NULL"
        
        try:
            result = self.cursor.execute(sql)
            return [student for student in result]
        except:
            print "Query Students' name failed"

    def display_no_of_student(self):
        print "There are %s students in classA" % self.no_of_student

    # Get #of students
    def get_no_of_student(self):
        sql = '''SELECT COUNT(DISTINCT sid) FROM classA WHERE sid IS NOT NULL'''
        try:
            result = self.cursor.execute(sql)
            no = [x[0] for x in result][0]
            return no
        except:
            print "getNoOfStudent failed!"

    # Get Score data 
    def __get_score_data(self):
        sql = '''SELECT name,score FROM classA'''
        try:
            data = self.cursor.execute(sql)
            score = [{}]*self.n_subject
            for x in data:
                name = x[0]
                score_data = str(x[1]).split("0x")[1:]

                for i in xrange(self.n_subject):
                    tag = int(score_data[self.n_tlv*i], 16)
                    length = int(score_data[self.n_tlv*i+1], 16)
                    if length == 4:
                        value = int(score_data[self.n_tlv*i+self.n_tlv-1], 16)
                    else:
                        value = 0
                    category = tag-1
                    score[category][name] = value
            return score
        except:
            print "__getScoreData failed!"

    def __get_category(self, subject):
        subject = subject.lower()
        category = []
        if subject in self.subject_map.values():
            if subject == 'chinese':
                category.append(1)
            elif subject == 'english':
                category.append(2)
            elif subject == 'math':
                category.append(3)
            else:  # all
                category = [i for i in xrange(4) if i > 0]
        return category
    
    # Display the highest Socre People by subject
    def display_highest_score(self, subject='all'):
        highest_score = self.get_highest_score(subject)
        category = self.__get_category(subject)
        if len(category) == 0:
            print "There's no subject:", subject
        else:
            msg = "The student %s has the highest score %s "

            if subject == 'all':
                for i in category:
                    print msg % highest_score[i-1] + " in subject %s" % self.subject_map[i]
            else:
                print msg % highest_score[0] + " in subject %s" % subject
    
    def get_highest_score(self, subject='all'):
        category = self.__get_category(subject)
        score = self.__get_score_data()
        highest_score = [sorted(score[i-1].items(), key=operator.itemgetter(1))[-1] for i in category]
        return highest_score

    # Display Ranking list
    def display_ranking_list(self):
        data = self.get_ranking_list()[::-1]
        for i in xrange(self.no_of_student):
            print "rank:", i+1, " name:%s total score:%s" % data[i]

    def get_ranking_list(self):
        score = self.__get_score_data()
        sum_score = {}
        sum_score = defaultdict(lambda: 0, sum_score)

        for i in xrange(self.n_subject):
            for k in score[i].keys():
                sum_score[k] += score[i][k]
        rank = sorted(sum_score.items(), key=operator.itemgetter(1))
        return rank

    # Display Students whose score are above score (default:60)
    def display_student_above_score(self, subject='all', min_score=60):
        data = self.get_student_above_score(subject, min_score)
        category = self.__get_category(subject)
        if len(category) == 1:
            print "Score >= %s" % min_score, "of Subject:%s" % self.subject_map[category[0]]
            for score in data[0]:
                print "Student: %s score: %s" % score 
        else:
            for i in category:
                print "Score >= %s" % min_score, "of Subject:%s" % self.subject_map[i]
                for score in data[i-1]:
                    print "Student: %s score: %s" % score 
                print "\n"
            
    def get_student_above_score(self, subject='all', min_score=60):
        data = self.__get_score_data()
        category = self.__get_category(subject)
        return [[score for score in data[i-1].items() if score[1] >= min_score] for i in category]