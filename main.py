import DBManager as mgr


class DisplayDBContent(object):
    def __init__(self):
        self.__mgr = mgr.DBManager('chrwu.db', 'classA')

    def insert_new_data(self, n_data=20, flag=False):
        self.__mgr.insert_data(n_data, flag)

    def display_student_names_by_gender(self, sex='all'):
        students = self.__mgr.get_student_names_by_gender(sex)
        print "There are %s students" % len(students) + " with gender %s :" % sex
        for student in students:
            print "Student's name: %s" % student[0]

    def display_no_of_student(self):
        print "There are %s students in classA" % self.__mgr.get_no_of_student()

    def display_highest_score(self, subject='all'):
        highest_score = self.__mgr.get_highest_score(subject)
        if len(highest_score) == 0:
            print "There's no data in database!"
            return
        category = self.__mgr.get_category(subject)
        if len(category) == 0:
            print "There's no subject:", subject
        else:
            msg = "The student %s has the highest score %s "

            subject_map = self.__mgr.get_subject_map()
            if subject == 'all':
                for i in category:
                    print msg % highest_score[i - 1] + " in subject %s" % subject_map[i]
            else:
                print msg % highest_score[0] + " in subject %s" % subject

    def display_ranking_list(self):
        data = self.__mgr.get_ranking_list()[::-1]
        for i in xrange(self.__mgr.get_no_of_student()):
            print "rank:", i + 1, " name:%s total score:%s" % data[i]

    def display_student_above_score(self, subject='all', min_score=60):
        data = self.__mgr.get_student_above_score(subject, min_score)
        if len(data) == 0:
            print "There's no data in database!"
            return
        category = self.__mgr.get_category(subject)
        subject_map = self.__mgr.get_subject_map()
        if len(category) == 1:
            print "Score >= %s" % min_score, "of Subject:%s" % subject_map[category[0]]
            for score in data[0]:
                print "Student: %s score: %s" % score
        else:
            for i in category:
                print "Score >= %s" % min_score, "of Subject:%s" % subject_map[i]
                for score in data[i - 1]:
                    print "Student: %s score: %s" % score
                print "\n"

if __name__ == '__main__':
    dsp = DisplayDBContent()

    

    # 0. Generate data and insert data into database
    dsp.insert_new_data(flag=False)

    # 1.Display students' name by giving gender (default: all gender)
    dsp.display_student_names_by_gender()
    dsp.display_student_names_by_gender('Male')
    dsp.display_student_names_by_gender('Female')

    # 2. Display # of students in class A
    dsp.display_no_of_student()

    # 3. Display the student's name who have the highest score by giving subject
    dsp.display_highest_score('chinese')
    dsp.display_highest_score('math')
    dsp.display_highest_score('english')
    dsp.display_highest_score('chemistry')
    dsp.display_highest_score('all')

    # 4. Display the ranking list in the class
    dsp.display_ranking_list()

    # 5. Display students' name whose score is above giving min. score and subject
    # (default: all subject & min=60)
    dsp.display_student_above_score(min_score=70)
    dsp.display_student_above_score(subject='math')


