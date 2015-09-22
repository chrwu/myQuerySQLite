import MyQuerySQLite as mq


# Display students' name by gender (default :F&M)
def display_student_names_by_gender(self, sex='all'):
    students = self.get_student_names_by_gender(sex)
    print "There are %s students" % len(students) + " with gender %s :" % sex
    for student in self.get_student_names_by_gender(sex):
        print "Student's name: %s" % student[0]


def display_no_of_student(self):
    print "There are %s students in classA" % self.no_of_student


# Display the highest Score People by subject
def display_highest_score(self, subject='all'):
    highest_score = self.get_highest_score(subject)
    category = self.get_category(subject)
    if len(category) == 0:
        print "There's no subject:", subject
    else:
        msg = "The student %s has the highest score %s "

        if subject == 'all':
            for i in category:
                print msg % highest_score[i - 1] + " in subject %s" % self.subject_map[i]
        else:
            print msg % highest_score[0] + " in subject %s" % subject


# Display Ranking list
def display_ranking_list(self):
    data = self.get_ranking_list()[::-1]
    for i in xrange(self.no_of_student):
        print "rank:", i + 1, " name:%s total score:%s" % data[i]


# Display Students whose score are above score (default:60)
def display_student_above_score(self, subject='all', min_score=60):
    data = self.get_student_above_score(subject, min_score)
    category = self.get_category(subject)
    if len(category) == 1:
        print "Score >= %s" % min_score, "of Subject:%s" % self.subject_map[category[0]]
        for score in data[0]:
            print "Student: %s score: %s" % score
    else:
        for i in category:
            print "Score >= %s" % min_score, "of Subject:%s" % self.subject_map[i]
            for score in data[i - 1]:
                print "Student: %s score: %s" % score
            print "\n"

if __name__ == '__main__':
    query = mq.MyQuerySQLite('chrwu.db', 'classA')

    # 0. Generate data and insert data into database
    data = query.generate_data()
    query.insert_data(data)

    # 1.Display students' name by giving gender (default: all gender)
    display_student_names_by_gender(query)
    display_student_names_by_gender(query, 'Male')
    display_student_names_by_gender(query, 'Female')

    # 2. Display # of students in class A
    display_no_of_student(query)

    # 3. Display the student's name who have the highest score by giving subject
    display_highest_score(query, 'chinese')
    display_highest_score(query, 'math')
    display_highest_score(query, 'english')
    display_highest_score(query, 'chemistry')
    display_highest_score(query, 'all')

    # 4. Display the ranking list in the class
    display_ranking_list(query)

    # 5. Display students' name whose score is above giving min. score and subject
    # (default: all subject & min=60)
    display_student_above_score(query, min_score=70)
    display_student_above_score(query, subject='math')

    query.close_db_connection()
