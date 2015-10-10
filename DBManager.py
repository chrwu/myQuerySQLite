import threading
from Queue import Queue
import SQLQuery as sqlq


class WorkerThread(threading.Thread):
    def __init__(self):
        super(WorkerThread, self).__init__()
        self.task_queue = Queue()
        self.close_event = threading.Event()

    def add_task(self, db_conn, task, *args, **kwarg):
        self.task_queue.put((db_conn, task, args, kwarg))

    def run(self):
        while not self.close_event.isSet():
            db_conn, func, args, kwarg = self.task_queue.get()
            result = kwarg['result']
            del kwarg['result']
            db_conn.db_connect()
            result.put(func(*args, **kwarg))
            #db_conn.db_disconnect()


class DBManager(WorkerThread):
    def __init__(self, db_name, db_table):
        super(DBManager, self).__init__()
        self.__query = sqlq.SQLQuery(db_name, db_table)
        self.__subject_map = None
        self._no_of_student = None  
        self.__result = Queue()
        self.start()

    @property
    def subject_map(self):
        if self.__subject_map is None:
            self.__subject_map = self.__query.subject_map
        return self.__subject_map

    @property
    def no_of_student(self):
        if self._no_of_student is None:
            self._no_of_student = self.get_no_of_student()
        return self._no_of_student

    def generate_data(self):
        self.add_task(self.__query, \
                      self.__query.generate_data, \
                      n_data=20, \
                      n_subjects=3, \
                      result=self.__result)
        return self.__result.get()

    def insert_data(self, data, flag=False):
        self.add_task(self.__query, \
                      self.__query.insert_data, \
                      data, \
                      flag=flag, \
                      result=self.__result)
        return self.__result.get()

    def get_student_names_by_gender(self, sex):
        self.add_task(self.__query, \
                      self.__query.get_student_names_by_gender, \
                      sex, \
                      result=self.__result)
        return self.__result.get()

    def get_no_of_student(self):
        self.add_task(self.__query, \
                      self.__query.get_no_of_student, \
                      result=self.__result)
        return self.__result.get()

    def get_category(self, subject):
        self.add_task(self.__query, \
                      self.__query.get_category, \
                      subject = subject, \
                      result=self.__result)
        return self.__result.get()

    def get_highest_score(self, subject='all'):
        self.add_task(self.__query, \
                      self.__query.get_highest_score, \
                      subject = subject, \
                      result=self.__result)
        return self.__result.get()

    def get_ranking_list(self):
        self.add_task(self.__query, \
                      self.__query.get_ranking_list, \
                      result=self.__result)
        return self.__result.get()

    def get_student_above_score(self, subject='all', min_score=60):
        self.add_task(self.__query, \
                      self.__query.get_student_above_score, \
                      subject='all', \
                      min_score=60, \
                      result=self.__result)
        return self.__result.get()
