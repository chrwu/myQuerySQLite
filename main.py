import MyQuerySQLite as mq

if __name__ == '__main__':
    query = mq.MyQuerySQLite('chrwu.db', 'classA')

    # 0. Generate data and insert data into database
    data = query.generate_data()
    query.insert_data(data)

    # 1.Display students' name by giving gender (default: all gender)
    query.display_student_names_by_gender()
    query.display_student_names_by_gender('Male')
    query.display_student_names_by_gender('Female')
    
    # 2. Display # of students in class A
    query.display_no_of_student()
    
    # 3. Display the student's name who have the highest score by giving subject
    query.display_highest_score('chinese')
    query.display_highest_score('math')
    query.display_highest_score('english')
    query.display_highest_score('chemistry')
    query.display_highest_score('all')
    
    # 4. Display the ranking list in the class
    query.display_ranking_list()
    
    # 5. Display students' name whose score is above giving min. score and subject
    # (default: all subject & min=60)
    query.display_student_above_score(min_score=70)
    query.display_student_above_score(subject='math')

    query.close_db_connection()
