from model.question_1 import Question_1
from model.question_2 import Question_2
from model.question_3 import Question_3
from model.question_4 import Question_4
from model.question_5 import Question_5


def runner_1(spark):

    print('Process Started For Question 1')

    obj = Question_1()
    df_dict = obj.ingest(spark)
    final_df = obj.process(df_dict)
    obj.export(final_df)

    print('Process Completed For Question 1')


def runner_2(spark):

    print('Process Started For Question 2')

    obj = Question_2()
    df_dict = obj.ingest(spark)
    final_df = obj.process(df_dict)
    obj.export(final_df)

    print('Process Completed For Question 2')


def runner_3(spark):

    print('Process Started For Question 3')

    obj = Question_3()
    df_dict = obj.ingest(spark)
    final_df = obj.process(df_dict)
    obj.export(final_df)

    print('Process Completed For Question 3')

def runner_4(spark):

    print('Process Started For Question 4')

    obj = Question_4()
    df_dict = obj.ingest(spark)
    final_df = obj.process(df_dict)
    obj.export(final_df)

    print('Process Completed For Question 4')

def runner_5(spark):

    print('Process Started For Question 5')

    obj = Question_5()
    df_dict = obj.ingest(spark)
    final_df = obj.process(df_dict)
    obj.export(final_df)

    print('Process Completed For Question 5')
