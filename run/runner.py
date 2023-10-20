from model.question_1 import Question_1
from model.question_2 import Question_2

def runner_1(spark):
    
    print('Process Started For Question 1')

    obj = Question_1()
    df_dict = obj.ingest(spark)
    final_df = obj.process(df_dict)
    obj.export(final_df)

    print('Process Completed For Question 1')

def runner_2(spark):
    
    print('Process Started For Question 1')

    obj = Question_2()
    df_dict = obj.ingest(spark)
    final_df = obj.process(df_dict)
    obj.export(final_df)

    print('Process Completed For Question 1')