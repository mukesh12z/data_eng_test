# 99 Group Data Engineering Technical Test

## Introduction

Part of 99 Groups data engineers tasks are to ensure that data pipelines are run in the correct order and in the most timely, reliable and scalable fashion. One example of this is our ELT data pipeline.

In the ELT data pipeline, data from different tables in the application database are first ingested _as-is_ into its respective source tables in the data warehouse. 

Thereafter, data in the source tables are cleaned, transformed, combined and stored into a final table (i.e. a datamart) where data analysts and business users will take their insights from. All these steps are done via SQL and executed via its respective SQL files.

In this test, you'll get a go at creating a part of this ELT data pipeline.

## Assignment Brief

In this assignment, you will create a program to execute different SQL files in the correct order. You can write your program in any language but note that we use mostly Python and Java in our stack.

__You are not allowed to use any specialized packages for this test. All functions and classes in this test should be written from scratch.__

In the [sql](./sql) folder, you'll find 3 subfolders related to the steps detailed in our introduction of the ELT data pipeline:

* [source](./sql/source) contains the source tables in the data warehouse.

* [tmp](./sql/tmp) contains SQL scripts used to clean, transform and combine the data in the data warehouse. You can assume that running a file in this folder will automatically create a table called `tmp.<sqlfile_basename>` in the data warehouse containing data from its sql logic.

* [final](./sql/final) contains SQL scripts used to create the final datamart. You can assume that running a file in this folder will automatically create a table called `final.<sqlfile_basename>` in the data warehouse containing data from its sql logic.

Note that each SQL file contains some dependencies that may require running of some other SQL files beforehand. Hence, you are required to write a program which contains functions to:

1. determine the dependency file(s) of each SQL file. You are required to print/show these dependencies as part of documenting the output.

2. run the SQL files in the correct order, in accordance to their dependencies. To simulate running of an SQL file, you can use `time.sleep(2)`. You are required to print/show the run order log as part of documenting the output.

3. __(Bonus)__ Do step (2) but execute the files in parallel while still respecting the order of dependencies.



## Deliverables

1. Ensure that your project has clear instructions on how to run them. Do also give a brief explanation on what you've coded and the results and include the print/logs required by the Assignment Brief.

2. We expect at least some unit tests, especially on critical functions of your codes.

3. Commit your project into a github private repository and share it with @luqmansrx and @zshaoz. Send a reply email to the recruiter to also let them know that you're done.
