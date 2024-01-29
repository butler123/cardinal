import pyodbc
import pandas as pd
import datetime

SERVER = 'butlertestserver123.database.windows.net'
DATABASE = 'CustomerRatingData'
USERNAME = 'serverlogin123'
PASSWORD = 'Database123'
connectionString = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}'

## connect to database
conn = pyodbc.connect(connectionString)
cursor = conn.cursor()

## in_int function applies to dataframes and removes all rows containing non-numeric values in the specified column
## this is used to remove rows where the customer number cannot be imported into the DB because of messy data
def is_int(s):
    try:
        float(s)
    except ValueError:
        return False
    else:
        return True


df1 = pd.read_csv("Customer Rating Agency A Inc.csv")
cust_num_count_a = df1[df1.columns[0]].count()
df1 = df1[df1['customer_number'].apply(is_int)]

df2 = pd.read_csv("Customer Rating Agency B Inc.csv")
cust_num_count_b = df2[df2.columns[0]].count()
df2 = df2[df2['customer_number'].apply(is_int)]

df3 = pd.read_csv("Customer Rating Agency C Inc.csv")
cust_num_count_c = df3[df3.columns[0]].count()
df3 = df3[df3['customer_number'].apply(is_int)]

df4 = pd.read_csv("Customer Rating Agency D Inc.csv")
cust_num_count_d = df4[df4.columns[0]].count()
df4 = df4[df4['customer_number'].apply(is_int)]

## array containing the counts of customer_number for the original datasets
cust_num_count_arr = [cust_num_count_a, cust_num_count_b, cust_num_count_c, cust_num_count_d]

## drop tables in the DB if they already exist
cursor.execute('''
    DROP TABLE IF EXISTS customer_rating_agency_a_inc
    ''')
cursor.execute('''
    DROP TABLE IF EXISTS customer_rating_agency_b_inc
    ''')
cursor.execute('''
    DROP TABLE IF EXISTS customer_rating_agency_c_inc
    ''')
cursor.execute('''
    DROP TABLE IF EXISTS customer_rating_agency_d_inc
    ''')

## create the DB tables
cursor.execute('''
    CREATE TABLE customer_rating_agency_a_inc (
        Customer_number int, 
        Customer_rating int, 
        Customer_rating_limit int, 
        Customer_status varchar(25)
        )
    ''')

cursor.execute('''
    CREATE TABLE customer_rating_agency_b_inc (
        Customer_number int, 
        Customer_rating int, 
        Customer_rating_limit int, 
        Customer_status varchar(25)
        )
    ''')

cursor.execute('''
    CREATE TABLE customer_rating_agency_c_inc (
        Customer_number int, 
        Customer_rating int, 
        Customer_rating_limit int, 
        Customer_status varchar(25)
        )
    ''')

cursor.execute('''
    CREATE TABLE customer_rating_agency_d_inc (
        Customer_number int, 
        Customer_rating int, 
        Customer_rating_limit int, 
        Customer_status varchar(25)
        )
    ''')

## insert the data into the database
for index, row in df1.iterrows():
    cursor.execute("INSERT INTO CustomerRatingData.dbo.customer_rating_agency_a_inc (Customer_number,Customer_rating,Customer_rating_limit, Customer_status) values(?,?,?,?)", \
    row.customer_number, row.customer_rating, row.customer_rating_limit, row.customer_status)

for index, row in df2.iterrows():
    cursor.execute("INSERT INTO CustomerRatingData.dbo.customer_rating_agency_b_inc (Customer_number,Customer_rating,Customer_rating_limit, Customer_status) values(?,?,?,?)", \
    row.customer_number, row.customer_rating, row.customer_rating_limit, row.customer_status)

for index, row in df3.iterrows():
    cursor.execute("INSERT INTO CustomerRatingData.dbo.customer_rating_agency_c_inc (Customer_number,Customer_rating,Customer_rating_limit, Customer_status) values(?,?,?,?)", \
    row.customer_number, row.customer_rating, row.customer_rating_limit, row.customer_status)

for index, row in df4.iterrows():
    cursor.execute("INSERT INTO CustomerRatingData.dbo.customer_rating_agency_d_inc (Customer_number,Customer_rating,Customer_rating_limit, Customer_status) values(?,?,?,?)", \
    row.customer_number, row.customer_rating, row.customer_rating_limit, row.customer_status)


conn.commit()


## perform the aggregations using the data in the database

customer_rating_sum_a = cursor.execute("SELECT SUM(customer_rating) FROM CustomerRatingData.dbo.customer_rating_agency_a_inc").fetchall()[0][0]
customer_rating_sum_b = cursor.execute("SELECT SUM(customer_rating) FROM CustomerRatingData.dbo.customer_rating_agency_b_inc").fetchall()[0][0]
customer_rating_sum_c = cursor.execute("SELECT SUM(customer_rating) FROM CustomerRatingData.dbo.customer_rating_agency_c_inc").fetchall()[0][0]
customer_rating_sum_d = cursor.execute("SELECT SUM(customer_rating) FROM CustomerRatingData.dbo.customer_rating_agency_d_inc").fetchall()[0][0]
customer_rating_sum_arr = [customer_rating_sum_a, customer_rating_sum_b, customer_rating_sum_c, customer_rating_sum_d]

loaded_cust_num_count_a = cursor.execute("SELECT COUNT(customer_number) FROM customer_rating_agency_a_inc").fetchall()[0][0]
loaded_cust_num_count_b = cursor.execute("SELECT COUNT(customer_number) FROM customer_rating_agency_b_inc").fetchall()[0][0]
loaded_cust_num_count_c = cursor.execute("SELECT COUNT(customer_number) FROM customer_rating_agency_c_inc").fetchall()[0][0]
loaded_cust_num_count_d = cursor.execute("SELECT COUNT(customer_number) FROM customer_rating_agency_d_inc").fetchall()[0][0]
loaded_cust_num_count_arr = [loaded_cust_num_count_a, loaded_cust_num_count_b, loaded_cust_num_count_c, loaded_cust_num_count_d]

high_value_count_a = cursor.execute("SELECT COUNT(*) FROM customer_rating_agency_a_inc WHERE customer_status = 'high-value'").fetchall()[0][0]
high_value_count_b = cursor.execute("SELECT COUNT(*) FROM customer_rating_agency_b_inc WHERE customer_status = 'high-value'").fetchall()[0][0]
high_value_count_c = cursor.execute("SELECT COUNT(*) FROM customer_rating_agency_c_inc WHERE customer_status = 'high-value'").fetchall()[0][0]
high_value_count_d = cursor.execute("SELECT COUNT(*) FROM customer_rating_agency_d_inc WHERE customer_status = 'high-value'").fetchall()[0][0]
high_value_count_arr = [high_value_count_a, high_value_count_b, high_value_count_c, high_value_count_d]

failed_customer_number_count_a = cust_num_count_a - loaded_cust_num_count_a
failed_customer_number_count_b = cust_num_count_b - loaded_cust_num_count_b
failed_customer_number_count_c = cust_num_count_c - loaded_cust_num_count_c
failed_customer_number_count_d = cust_num_count_d - loaded_cust_num_count_d
failed_customer_number_count_arr = [failed_customer_number_count_a, failed_customer_number_count_b, failed_customer_number_count_c, failed_customer_number_count_d]


failed_customer_number_percentage_arr = []
for i in range(len(failed_customer_number_count_arr)):
    failed_customer_number_percentage_arr.append(failed_customer_number_count_arr[i] / cust_num_count_arr[i] * 100)

customer_rating_agency_arr = ['customer_rating_agency_a_inc', 'customer_rating_agency_b_inc', 'customer_rating_agency_c_inc', 'customer_rating_agency_d_inc']

data = {'customer_rating_agency': customer_rating_agency_arr, 'customer_number_count': loaded_cust_num_count_arr, 'customer_rating_sum': customer_rating_sum_arr, \
    'high_value_count': high_value_count_arr, 'failed_customer_number_count': failed_customer_number_count_arr, 'failed_customer_number_percentage': failed_customer_number_percentage_arr}


## create the dataframe for the output report
output_df = pd.DataFrame(data)



today = datetime.datetime.today().strftime('%Y%m%d')
filename = 'Customer_Rating_Aggregate_Report_' + today + '.csv'

## output the report to a csv file
output_df.to_csv(filename, index=False)
