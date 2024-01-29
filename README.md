# README

This repo contains a python file named "pipeline.py" which gets run by a Github workflow everyday at 5pm. 
The file contains scripts which read the source csv files into Pandas dataframes, then it connects to a MS SQL Server database named "CustomerRatingData" and creates the ratings tables. Then the data is loaded from the dataframes into the SQL database, aggregations are performed on the data, and then the results are output into a csv file.




