import operator
import sqlite3
import random as rd
from collections import defaultdict


class MyQuerySQLite(object):
    def __init__(self, db_name, db_table):
        self.n_subject = 3
        self.n_tlv = 6
        self.subject_map = {0: 'all', 1: 'chinese', 2: 'english', 3: 'math'}
        self.db_name = db_name
        self.db_table = db_table

    def db_connect(self):
        self.conn = sqlite3.connect(self.db_name)
        self.cursor = self.conn.cursor()
        self.__create_table(self.db_table)
        self.no_of_student = self.get_no_of_student()

    def db_disconnect(self):
        self.cursor.close()
        self.conn.close()

    def __create_table(self, table_name='classA'):
        sql = '''CREATE TABLE if not exists %s (sid INTERGER, name TEXT, gender TEXT, score BLOB)''' % table_name
        try:
            result = self.cursor.execute(sql)
        except:
            print "create new table failed!"
        return result

    def generate_data(self, n_data=20, n_subjects=3):
        data = []
        for i in xrange(n_data):
            n_score = ''
            length = str(hex(4)) + str(hex(0)) + str(hex(0)) + str(hex(0))
            for j in xrange(n_subjects):
                score = rd.randint(0, 100)
                tag = str(hex(j + 1))
                value = str(hex(score))
                n_score += tag + length + value
            data.append((i + 1, 'a' + str(i + 1), rd.choice('MF'), sqlite3.Binary(n_score)))
        return data

    def insert_data(self, data, flag=False):
        if flag:
            inserted_data = []
            for i in xrange(len(data)):
                sid = data[i][0] + self.no_of_student
                name = data[i][1][0] + str(int(data[i][1][1:]) + self.no_of_student)
                inserted_data.append((sid, name, data[i][2], data[i][3]))
            try:
                self.cursor.executemany('INSERT INTO classA VALUES (?, ?, ?, ?)', inserted_data)
                self.conn.commit()
            except:
                print 'Insert data failed!'

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
        except:
            print "Query Students' name failed"
        return [student for student in result]

    # Get #of students
    def get_no_of_student(self):
        sql = '''SELECT COUNT(DISTINCT sid) FROM classA WHERE sid IS NOT NULL'''
        try:
            result = self.cursor.execute(sql)
            no = [x[0] for x in result][0]
        except:
            print "getNoOfStudent failed!"
        return no

    # Get Score data 
    def __get_score_data(self):
        sql = '''SELECT name,score FROM classA'''
        try:
            data = self.cursor.execute(sql)
        except:
            print "__getScoreData failed!"

        score = [{}] * self.n_subject
        for x in data:
            name = x[0]
            score_data = str(x[1]).split("0x")[1:]

            for i in xrange(self.n_subject):
                tag = int(score_data[self.n_tlv * i], 16)
                length = int(score_data[self.n_tlv * i + 1], 16)
                if length == 4:
                    value = int(score_data[self.n_tlv * i + self.n_tlv - 1], 16)
                else:
                    value = 0
                category = tag - 1
                score[category][name] = value
        return score

    # get subject's category
    def get_category(self, subject):
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

    # get the highest score by subject (default: all subjects resp.)
    def get_highest_score(self, subject='all'):
        category = self.get_category(subject)
        score = self.__get_score_data()
        if score.count({}) == len(score):
            return []
        else:
            highest_score = [sorted(score[i - 1].items(), key=operator.itemgetter(1))[-1] for i in category]
            return highest_score

    # get the ranking list 
    def get_ranking_list(self):
        score = self.__get_score_data()
        
        sum_score = {}
        sum_score = defaultdict(lambda: 0, sum_score)

        if score.count({}) == len(score):
            return []
        else:
            for i in xrange(self.n_subject):
                for k in score[i].keys():
                    sum_score[k] += score[i][k]
            rank = sorted(sum_score.items(), key=operator.itemgetter(1))
            return rank

    # get the students' list whose score above the input score
    def get_student_above_score(self, subject='all', min_score=60):
        data = self.__get_score_data()
        if data.count({}) == len(data):
            return []
        else:
            category = self.get_category(subject)
            return [[score for score in data[i - 1].items() if score[1] >= min_score] for i in category]