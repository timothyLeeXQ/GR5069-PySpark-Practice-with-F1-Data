# F1 Dataset Analysis Notebook

Started on 21 Feb 2020

## 21 Feb 2020:
* Created basic folder structure
* Created initial ipynb report with answers to some questions sought for assignment 1.
* Populated project resources, documentation, etc.

### Notes on outstanding issues

#### Question 3
No idea what it means to "insert the missing code (e.g: ALO for Alonso) for drivers based on the
 'drivers' dataset". This is an outstanding issue in the [TODO](TODO.txt) list, and I have put out a question on slack to seek clarification.

 I thought it meant that there was missing driver code data somewhere, but the driver code (ALO, BUT, MAS) is present only in one table - drivers.csv - and contains no missing data.

 The other possibility is that we are being asked to include the driver code in the joined table that for question 1, and could be reused (and grown) to address 2. This was something I had already done in questions 1 and 2. If this turns out to be what we needed to do, no further action is needed, except to point the instructors to scroll up.

#### Structure of reports
The report for assignment 1 does not explicitly answer most of the questions asked (as a statement or summary). Rather, it presents data tables as a response to each question .This was done as most of the questions don't lend themselves to textual answers but are best presented in a tabular format (e.g. asking for one particular driver for each race, when there are hundreds of races).

In most cases, I have endeavoured to present each data table with the relevant information available just by looking at the table, or with some scrolling. Only columns of interest are presented for easy viewing, and sorting is done where it makes sense. There are two exceptions to this:

1. In some cases, the question phrasing gave the impression that questions were intended for the student to join tables or create columns, accumulating them in a central table of data. In these cases , the full table is presented, rather than just the columns of interest.
2. In the case of question 5, it is possible to provide a table that better answers the question of who had the most wins/losses at each race. Right now, all wins and losses in each race are presented, not just the biggest winners/losers. A better solution would be to present each race as a row, and the top winner/loser as columns. I have added this to the  document, but probably won't be able to get to it before Wednesday.

#### Scripting
[`src`](src) is currently empty. I would have liked to create scripts to run the S3 bucket mount and the col_to_int_type() function defined in the assignment notebook - this would be more proper. However, to do this I need to import the scripts stored in /src from /reports - Not sure how to do this. This has also been added to the [TODO](TODO.txt).
