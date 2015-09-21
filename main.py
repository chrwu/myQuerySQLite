import myQuerySQLite 

if __name__ == '__main__':
    mqsqlite = myQuerySQLite('chrwu.db','classA')
    data = mqsqlite.genData()
    mqsqlite.insertData(data)

    # 1.Display students' name by giving gender (default: all gender)
    mqsqlite.displayStudentNamesByGender()
    mqsqlite.displayStudentNamesByGender('Male')
    mqsqlite.displayStudentNamesByGender('Female')
    
    # 2. Display # of students in class A
    mqsqlite.displayNoOfStudent()
    
    # 3. Display the student's name who have the highest score by giving subject
    mqsqlite.displayHighestScore('chinese')
    mqsqlite.displayHighestScore('math')
    mqsqlite.displayHighestScore('english')
    mqsqlite.displayHighestScore('all')
    
    # 4. Display the ranking list in the class
    mqsqlite.displayRankingList()
    
    # 5. Display students' name whose score is above giving min. score and subject 
    # (default: all subject & min=60)
    mqsqlite.displayStudentAboveScore(abscore=70)
    mqsqlite.displayStudentAboveScore(subject='math')

    mqsqlite.closeDBConnection()
