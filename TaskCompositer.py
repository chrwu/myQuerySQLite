import sqlite3
from collections import defaultdict
import operator
import random as rd


class QueryTask(object):
    def __init__(self, db_name, db_table, sql, result):
        self.__conn = None
        self.__cursor = None
        self.__db_name = db_name
        self.__db_table = db_table
        self.__sql = sql
        self.__result = result

    def query_connect(self):
        if self.__conn is None:
            self.__conn = sqlite3.connect(self.__db_name)
        if self.__cursor is None:
            self.__cursor = self.__conn.cursor()
        self.__create_table(self.__db_table)

    def __create_table(self, table_name='classA'):
        sql = '''CREATE TABLE if not exists %s (sid INTERGER, name TEXT, gender TEXT, score BLOB)''' % table_name
        
        try:
            self.__cursor.execute(sql)
        except Exception as e:
            print "__create_table fail"
            print e

    def query_disconnect(self):
        self.__cursor.close()
        self.__conn.close()

    def commit_many(self, sql, data):
        self.__cursor.executemany(sql, data)
        self.__conn.commit()

    def execute(self):
        self.query_connect()
        result = []
        obj_sqlite = None
        try:
            obj_sqlite = self.__cursor.execute(self.__sql)
            result = [item for item in obj_sqlite]
        except Exception as e:
            print "QueryTask execute() failed! %s" % e
        
        self.__result.put(result)
        self.query_disconnect()


class ImportTask(QueryTask):
    def __init__(self, db_name, db_table, sql, result, import_sql, flag, data):
        super(ImportTask, self).__init__(db_name, db_table, sql, result)
        self.__import_sql = import_sql
        self.__data = data

    def execute(self):
        try:
            self.query_connect()
            self.commit_many(self.__import_sql, self.__data)
            self.query_disconnect()
        except Exception as e:
            print e

class DataGetTask(object):
    def __init__(self, func_name, n_data, no_of_student, result):
        self.__func_name = func_name
        self.__n_data = n_data
        self.__no_of_student = no_of_student
        self.__result = result
        self.__n_subject = 3

    def execute(self):
        if self.__func_name == 'generate_data':
            result = self.generate_data()
        self.__result.put(result)

    def __generate_raw_data(self, n_data):
        data = []
        for i in xrange(n_data):
            n_score = ''
            length = str(hex(4)) + str(hex(0)) + str(hex(0)) + str(hex(0))
            for j in xrange(self.__n_subject):
                score = rd.randint(0, 100)
                tag = str(hex(j + 1))
                value = str(hex(score))
                n_score += tag + length + value
            data.append((i + 1, 'a' + str(i + 1), rd.choice('MF'), sqlite3.Binary(n_score)))
        return data

    def __process_insert_data(self, data):
        inserted_data = []
        for i in xrange(len(data)):
            sid = data[i][0] + self.__no_of_student
            name = data[i][1][0] + str(int(data[i][1][1:]) + self.__no_of_student)
            inserted_data.append((sid, name, data[i][2], data[i][3]))
        return inserted_data

    def generate_data(self, n_data=20):
        insert_data = []
        data = []
        try:
            data = self.__generate_raw_data(n_data)
            insert_data = self.__process_insert_data(data)
        except Exception as e:
            print e
        return insert_data      


class DataProcessTask(object):
    def __init__(self, func_name, data, subject, min_score, result):
        self.__func_name = func_name
        self.__data = data
        self.__subject = subject
        self.__min_score = min_score
        self.__result = result
        self.__subject_map = None
        self.__n_subject = 3
        self.__n_tlv = 6

    def execute(self):
        result = []
        if self.__func_name == 'process_no_of_student':
            result = self.process_no_of_student()
        elif self.__func_name == 'process_score_data':
            result = self.process_score_data()
        elif self.__func_name == 'process_highest_score':
            result = self.process_highest_score(self.__subject)
        elif self.__func_name == 'process_ranking_list':
            result = self.process_ranking_list()
        elif self.__func_name == 'process_student_above_score':
            result = self.process_student_above_score(self.__subject, self.__min_score)
        elif self.__func_name == 'get_category':
            result = self.get_category(self.__subject)
        elif self.__func_name == 'get_subject_map':
            result = self.subject_map
        else:
            print "There's no function: ", self.__func_name
            
        self.__result.put(result)

    @property
    def subject_map(self):
        if self.__subject_map is None:
            self.__subject_map = {0: 'all', 1: 'chinese', 2: 'english', 3: 'math'}
        return self.__subject_map

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

    def process_no_of_student(self):
        no = 0
        try:
            no = [x[0] for x in self.__data][0]
        except Exception as e:
            print e
        return no

    def process_score_data(self):
        score = [{}] * self.__n_subject
        
        for x in self.__data:
            name = x[0]
            score_data = str(x[1]).split("0x")[1:]

            for i in xrange(self.__n_subject):
                tag = int(score_data[self.__n_tlv * i], 16)
                length = int(score_data[self.__n_tlv * i + 1], 16)
                if length == 4:
                    value = int(score_data[self.__n_tlv * i + self.__n_tlv - 1], 16)
                else:
                    value = 0
                category = tag - 1
                score[category][name] = value
        return score

    def process_highest_score(self, subject):
        category = self.get_category(subject)
        if self.__data.count({}) == len(self.__data):
            return []
        else:
            highest_score = [sorted(self.__data[i - 1].items(), key=operator.itemgetter(1))[-1] for i in category]
            return highest_score

    def process_ranking_list(self):
        sum_score = {}
        sum_score = defaultdict(lambda: 0, sum_score)
        if self.__data.count({}) == len(self.__data):
            return []
        else:
            for i in xrange(self.__n_subject):
                for k in self.__data[i].keys():
                    sum_score[k] += self.__data[i][k]
            rank = sorted(sum_score.items(), key=operator.itemgetter(1))
            return rank

    def process_student_above_score(self, subject='all', min_score=60):
        if self.__data.count({}) == len(self.__data):
            return []
        else:
            category = self.get_category(subject)
            return [[score for score in self.__data[i - 1].items() if score[1] >= min_score] for i in category]


class TaskCompositer(object):
    def __init__(self, db_name, db_table, result):
        self.__db_name = db_name
        self.__db_table = db_table
        self.__result = result

    def generate_data(self, n_data, no_of_student):
        task = DataGetTask(func_name='generate_data', \
                           n_data=n_data, \
                           no_of_student=no_of_student, \
                           result=self.__result)
        return task

    def insert_data(self, flag, data):
        import_sql = '''INSERT INTO classA VALUES (?, ?, ?, ?)'''
        task = ImportTask(db_name=self.__db_name, \
                          db_table=self.__db_table, \
                          sql=None, \
                          result=self.__result, \
                          import_sql=import_sql, \
                          flag=flag, \
                          data=data)
        return task

    def get_category(self, subject):
        task = DataProcessTask(func_name='get_category', \
                               data=[], \
                               subject=subject, \
                               min_score=None, \
                               result=self.__result)
        return task

    def get_subject_map(self):
        task = DataProcessTask(func_name='get_subject_map', \
                               data=[], \
                               subject=None, \
                               min_score=None, \
                               result=self.__result)
        return task

    def get_student_names_by_gender(self, sex):
        sql = '''SELECT name, gender FROM classA WHERE gender '''
        if sex == 'Female':
            sql += "= \'F\'"
        elif sex == 'Male':
            sql += "= \'M\'"
        else:
            sql += "IS NOT NULL"

        task = QueryTask(db_name=self.__db_name, \
                         db_table=self.__db_table, \
                         sql=sql, \
                         result=self.__result)
        return task

    def query_no_of_student(self):
        sql = '''SELECT COUNT(DISTINCT sid) FROM classA WHERE sid IS NOT NULL'''
        task = QueryTask(db_name=self.__db_name, \
                         db_table=self.__db_table, \
                         sql=sql, \
                         result=self.__result)
        return task

    def get_score_data(self):
        sql = '''SELECT name,score FROM classA'''
        task = QueryTask(db_name=self.__db_name, \
                         db_table=self.__db_table, \
                         sql=sql, \
                         result=self.__result)
        return task

    def process_no_of_student(self, data):
        task = DataProcessTask(func_name='process_no_of_student', \
                               data=data, \
                               subject=None, \
                               min_score=None, \
                               result=self.__result)
        return task

    def process_score_data(self, data):
        task = DataProcessTask(func_name='process_score_data', \
                               data=data, \
                               subject=None, \
                               min_score=None, \
                               result=self.__result)
        return task

    def process_highest_score(self, subject, data):
        task = DataProcessTask(func_name='process_highest_score', \
                        data=data, \
                        subject=subject, \
                        min_score=None, \
                        result=self.__result)
        return task

    def process_ranking_list(self, data):
        task = DataProcessTask(func_name='process_ranking_list', \
                        data=data, \
                        subject=None, \
                        min_score=None, \
                        result=self.__result)
        return task

    def process_student_above_score(self, data, subject, min_score=60):
        task = DataProcessTask(func_name='process_student_above_score', \
                        data=data, \
                        subject=subject, \
                        min_score=min_score, \
                        result=self.__result)
        return task