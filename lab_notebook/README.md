# F1 Dataset Analysis Notebook

Started on 21 Feb 2020

## 21 Feb 2020:
* Created basic folder structure
* Created initial ipynb report with answers to some questions sought for assignment 1.
* Populated project resources, documentation, etc.

### Notes

#### Structure of reports
The report for assignment 1 does not explicitly answer most of the questions asked (as a statement or summary). Rather, it presents data tables as a response to each question. This was done as most of the questions don't lend themselves to textual answers but are best presented in a tabular format (e.g. asking for one particular driver for each race, when there are hundreds of races).

In most cases, I have endeavoured to present each data table with the relevant information available just by looking at the table, or with some scrolling. Only columns of interest are presented for easy viewing, and sorting is done where it makes sense. There are two exceptions to this:

1. In some cases, the question phrasing gave the impression that questions were intended for the student to join tables or create columns, accumulating them in a central table of data. In these cases , the full table is presented, rather than just the columns of interest.
2. In the case of question 5, it is possible to provide a table that better answers the question of who had the most wins/losses at each race. Right now, all wins and losses in each race are presented, not just the biggest winners/losers. A better solution would be to present each race as a row, and the top winner/loser as columns. I have added this to the  document, but probably won't be able to get to it before Wednesday.

### Outstanding

#### Question 3
No idea what it means to "insert the missing code (e.g: ALO for Alonso) for drivers based on the
 'drivers' dataset". This is an outstanding issue in the TODO list, and I have put out a question on slack to seek clarification.

 I thought it meant that there was missing driver code data somewhere, but the driver code (ALO, BUT, MAS) is present only in one table - drivers.csv - and contains no missing data.

 The other possibility is that we are being asked to include the driver code in the joined table that for question 1, and could be reused (and grown) to address 2. This was something I had already done in questions 1 and 2. If this turns out to be what we needed to do, no further action is needed, except to point the instructors to scroll up.

#### Scripting
`src` is currently empty. I would have liked to create scripts to run the S3 bucket mount and the col_to_int_type() function defined in the assignment notebook - this would be more proper. However, to do this I need to import the scripts stored in /src from /reports - Not sure how to do this. This has also been added to the.

## 22 Feb 2020:
* Fixed Question 3 issue

### Notes
#### On question 3
Turns out there were missing driver codes - these were reflected as "\N" in the data, as was true for other missing values. Missing driver codes were replaced with the first 3 letters of their last name, as is the convention for F1 drivers' codes. A few exceptions were mentioned in the report, usually arising from the first 3 letters of the last name being shared between two or more drivers. These were not dealt with. Chasing down exceptions would've been a tedious manual endeavour given the myriad of ways that FIA resolved these conflicts.

## 25 Feb 2020:
* Fixed Question 5
* Created mount link to xql2001 S3 bucket.
* Added commands to write question response tables to xql2001 S3 bucket
* Shortened variable names in ipynb

### Notes
A few important things were clarified (I think).

#### Answers to be submitted
We are to present our responses as CSV tables written to the S3 bucket. Ideally, this would be done in code.

In light of this, I have created code to mount the newly created xql2001-gr5069 S3 bucket, polished the code (mostly by shortening obnoxiously long variable names), and had the tables that explicitly provide answers written into the xql2001 S3 bucket.

I also took the opportunity to fix the issue noted above about question 5. The response now comes in two separate DFs - one that shows the drivers with the most wins (and how many) for a given circuit, and one that does the same for losses. This has been removed from TODO.

#### src
The source code to be accepted is the ipynb file exported from databricks. The S3 mount code doesn't work outside databricks, so you needed to work on a notebook there anyway, which means that as long as you wanted to work with direct read/write access to S3, your code has to be in notebook form, not script form.

So after cleaning up the code in databricks, I exported the file and moved it into the src folder. The reports folder is now empty and hence absent from the Github repo.

I've not removed the issues related to do this from TODO, because I feel this solution is still not ideal and I'm still missing something...


## 26 Feb 2020:
* Fixed issue where csv is saved in multiple parts on S3

### Notes

#### Writing CSVs in Spark
From today's class and exchange on slack, I discovered that csv tables written using Pyspark's .write.csv() method created many CSV file parts rather than a single large CSV. This has something to do with Spark's parallel processing capabilities. The issue can be, and was, resolved using the coalesce(1) function, as we were advised.

**For overwritting S3 files in the future - the old files needed to be deleted on the S3 bucket for the new, coalesced files to be written.**
