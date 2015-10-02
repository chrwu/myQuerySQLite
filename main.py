import MyQuerySQLite as mq


class DisplayDBContent(object):
    def __init__(self):
        self.__query = mq.MyQuerySQLite('chrwu.db', 'classA')

    def open_connection(self):
        self.__query.db_connect()

    def close_connection(self):
        self.__query.db_disconnect()

    def insert_new_data(self, is_insert=False):
        data = self.__query.generate_data()
        self.__query.insert_data(data, flag=is_insert)

    # Display students' name by gender (default :F&M)
    def display_student_names_by_gender(self, sex='all'):
        students = self.__query.get_student_names_by_gender(sex)
        print "There are %s students" % len(students) + " with gender %s :" % sex
        for student in students:
            print "Student's name: %s" % student[0]

    def display_no_of_student(self):
        print "There are %s students in classA" % self.__query.no_of_student

    # Display the highest Score People by subject
    def display_highest_score(self, subject='all'):
        highest_score = self.__query.get_highest_score(subject)
        if len(highest_score) == 0:
            print "There's no data in database!"
            return
        category = self.__query.get_category(subject)
        if len(category) == 0:
            print "There's no subject:", subject
        else:
            msg = "The student %s has the highest score %s "

            if subject == 'all':
                for i in category:
                    print msg % highest_score[i - 1] + " in subject %s" % self.__query.subject_map[i]
            else:
                print msg % highest_score[0] + " in subject %s" % subject

    # Display Ranking list
    def display_ranking_list(self):
        data = self.__query.get_ranking_list()[::-1]
        for i in xrange(self.__query.no_of_student):
            print "rank:", i + 1, " name:%s total score:%s" % data[i]

    # Display Students whose score are above score (default:60)
    def display_student_above_score(self, subject='all', min_score=60):
        data = self.__query.get_student_above_score(subject, min_score)
        if len(data) == 0:
            print "There's no data in database!"
            return
        category = self.__query.get_category(subject)
        if len(category) == 1:
            print "Score >= %s" % min_score, "of Subject:%s" % self.__query.subject_map[category[0]]
            for score in data[0]:
                print "Student: %s score: %s" % score
        else:
            for i in category:
                print "Score >= %s" % min_score, "of Subject:%s" % self.__query.subject_map[i]
                for score in data[i - 1]:
                    print "Student: %s score: %s" % score
                print "\n"

if __name__ == '__main__':
    obj = DisplayDBContent()

    # open db connection
    obj.open_connection()

    # 0. Generate data and insert data into database
    obj.insert_new_data(is_insert=False)

    # 1.Display students' name by giving gender (default: all gender)
    obj.display_student_names_by_gender()
    obj.display_student_names_by_gender('Male')
    obj.display_student_names_by_gender('Female')

    # 2. Display # of students in class A
    obj.display_no_of_student()

    # 3. Display the student's name who have the highest score by giving subject
    obj.display_highest_score('chinese')
    obj.display_highest_score('math')
    obj.display_highest_score('english')
    obj.display_highest_score('chemistry')
    obj.display_highest_score('all')

    # 4. Display the ranking list in the class
    obj.display_ranking_list()

    # 5. Display students' name whose score is above giving min. score and subject
    # (default: all subject & min=60)
    obj.display_student_above_score(min_score=70)
    obj.display_student_above_score(subject='math')

    # close db connection
    obj.close_connection()

