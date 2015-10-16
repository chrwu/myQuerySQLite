import TaskCompositer as tc
import threading
from Queue import Queue


class WorkerThread(threading.Thread):
    def __init__(self):
        super(WorkerThread, self).__init__()
        self.task_queue = Queue()
        self.close_event = threading.Event()

    def add_task(self, task):
        self.task_queue.put(task)

    def run(self):
        while not self.close_event.isSet():
            task = self.task_queue.get()
            task.execute()
            self.task_queue.task_done()


class DBManager(WorkerThread):
    def __init__(self, db_name, db_table):
        super(DBManager, self).__init__()
        self.__result = Queue()
        self.__task_compostiter = tc.TaskCompositer(db_name, db_table, self.__result)
        self.start()

    def get_subject_map(self):
        tasks = self.__task_compostiter.get_subject_map()
        self.add_task(tasks)
        return self.__result.get()

    def get_category(self, subject='all'):
        tasks = self.__task_compostiter.get_category(subject)
        self.add_task(tasks)
        return self.__result.get()

    def insert_data(self, n_data=20, flag=False):
        no_of_student = self.get_no_of_student()
        tasks = self.__task_compostiter.generate_data(n_data, no_of_student)
        self.add_task(tasks)
        data = self.__result.get()
        tasks = self.__task_compostiter.insert_data(flag, data)
        self.add_task(tasks)

    def get_student_names_by_gender(self, sex='all'):
        tasks = self.__task_compostiter.get_student_names_by_gender(sex)
        self.add_task(tasks)
        return self.__result.get()

    def get_no_of_student(self, sex='all'):
        tasks = self.__task_compostiter.query_no_of_student()
        self.add_task(tasks)
        data = self.__result.get()
        tasks = self.__task_compostiter.process_no_of_student(data)
        self.add_task(tasks)
        return self.__result.get()

    def get_score_data(self):
        tasks = self.__task_compostiter.get_score_data()
        self.add_task(tasks)
        data = self.__result.get()
        tasks = self.__task_compostiter.process_score_data(data)
        self.add_task(tasks)
        return self.__result.get()

    def get_highest_score(self, subject='all'):
        data = self.get_score_data()
        tasks = self.__task_compostiter.process_highest_score(subject, data)
        self.add_task(tasks)
        return self.__result.get()

    def get_ranking_list(self):
        data = self.get_score_data()
        tasks = self.__task_compostiter.process_ranking_list(data)
        self.add_task(tasks)
        return self.__result.get()

    def get_student_above_score(self, subject='all', min_score=60):
        data = self.get_score_data()
        tasks = self.__task_compostiter.process_student_above_score(data, subject, min_score)
        self.add_task(tasks)
        return self.__result.get()
        

#if __name__ == '__main__':
#    mgr = DBManager('chrwu.db', 'classA')
#    mgr.insert_data(flag=True)