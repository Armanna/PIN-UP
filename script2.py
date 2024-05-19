# import packages
import os
import glob
import logging
import pandas as pd
from datetime import datetime


# Basic configuration
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Create a logger
bets_logger = logging.getLogger('bets')
payments_logger = logging.getLogger('payments')

# Data Manipulation Techniques
def date_normalizer_1(date_time_str:str):
    if date_time_str is None:
        return date_time_str


    if 'AM' not in date_time_str and 'PM' not in date_time_str:
        return date_time_str

    # Parse the string into a datetime object
    try:
        date_time_obj = datetime.strptime(date_time_str, '%d%m%Y %I:%M %p')
    except:
        date_time_str = date_time_str.replace(' PM', '')[:14]
        return date_time_str

    # Convert the datetime object to the desired format string
    formatted_date_time_str = date_time_obj.strftime('%d/%m/%Y %H:%M')

    return formatted_date_time_str



def date_normalizer_2(date_str):

    date_format = "%m/%d/%Y %H:%M"
    date_obj = datetime.strptime(date_str, date_format)
    normalized_date_str = date_obj.isoformat()

    return normalized_date_str


def to_datetime(date_string:str):
    datetime_object = datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S")

    return datetime_object

def to_seconds(obj):
    if obj != '-':
        return obj.total_seconds()
    return 999999999

###############################################################################
############################## DATA EXTRACTION ################################
###############################################################################


# get the file path
directoryPath = os.path.abspath(os.getcwd())

bets_files = directoryPath + '\\bets\*.csv'
payments_files = directoryPath + '\\payments\*.csv'

# extract bets data into dataframe
bets = pd.DataFrame()
for file_name in glob.glob(bets_files):
    x = pd.read_csv(file_name, low_memory=False)
    bets = pd.concat([bets,x],axis=0)


# print(bets.head())
bets_rows = len(bets)
bets_logger.info(f'bets data has {bets_rows} bets')

# extract payments data into dataframe
payments = pd.DataFrame()
for file_name in glob.glob(payments_files):
    x = pd.read_csv(file_name, low_memory=False)
    payments = pd.concat([payments,x],axis=0)

# print(payments.head())
payments_rows = len(payments)
payments_logger.info(f'payments data has {payments_rows} payments')

print('-----------------------------------------------------------------------')
###############################################################################
#################### DATA QUALITY CHECK IN HIGH LEVEL##########################
###############################################################################


bets_info = bets.isnull().sum().to_dict()

for key, value in bets_info.items():
    if value > 0:
        bets_logger.info(f'{key} has {value} null values')



payments_info = bets.isnull().sum().to_dict()

for key, value in payments_info.items():
    if value > 0:
        payments_logger.info(f'{key} has {value} null values')



print('-----------------------------------------------------------------------')


###############################################################################
############################ DATA TRANSFORMATION ##############################
###############################################################################

# We are going to build a history which will contain user actions (deposit, bet, withdraw)
# We need to transform both dataframes like a way to union them
# Then order them by player_id and date and solve the tasks

bets['transaction_type'] = 'bet'
bets['comment'] = None
bets['payment_method_name'] = None
bets['status'] = None
bets.rename(columns = {'accept_time':'Date'}, inplace = True)

payments['result'] = None
payments['payout'] = None
payments.rename(columns = {'paid_amount':'amount'}, inplace = True)

# As bets has some missing Date values, let's close them with max Date.
# In ideal project I would like to get the missing Date values to keep Data quality in high level.
bets['Date'] = bets['Date'].fillna('12/31/9999 00:00')
bets['Date'] = bets['Date'].apply(date_normalizer_1)
bets['Date'] = bets['Date'].apply(date_normalizer_2)


payments['Date'] = payments['Date'].apply(date_normalizer_1)
payments['Date'] = payments['Date'].apply(date_normalizer_2)

# keep only status in ('Approveed', 'Completed', 'Issued') and amount is not error and comment is not error
payments = payments[payments['status'].isin(['Approved', 'Completed', 'Issued'])]
payments = payments[~payments['amount'].isin(['error'])]
payments = payments[~payments['comment'].isin(['error'])]

bets =  bets[['player_id','Date','transaction_type','amount','result','payout','comment','payment_method_name','status']]
payments = payments[['player_id','Date','transaction_type','amount','result','payout','comment','payment_method_name','status']]

history = pd.concat([bets,payments])

# make amount to float
history['amount'] = history['amount'].apply(float)

# make player_id all integer, in case of non numeric values -> remove
history['player_id'] = history['player_id'].astype(str)
history = history[history['player_id'].str.isnumeric()]
history['player_id'] = history['player_id'].astype(int)


###############################################################################
################################### SOLUTION ##################################
###############################################################################



# Order by player id and date in ascending order
history.reset_index(drop=True, inplace=True)

history = history.sort_values(by=['player_id', 'Date'], ascending=[True, True])

# print(history)

# history.to_excel('result.xlsx')

# TASK 1

# 1.1) на основании данных в папках payments и bets, находит клиента, который совершил такую последовательность операций:
#        1.1.1) депозит;
#        1.1.2) ставка на сумму депозита +-10%;
#        1.1.3) вывод в течении часа от депозита через систему, отличную от депозита;
# 1.2) сохранить результат в папке result с названием "resultSSMMDDMMYYYY.csv";

# It would be nice to use SQL here, especially window functions ))) Buy anyway let's go with python

history['Date'] = history['Date'].apply(to_datetime)

history['next_transaction_type'] = history.groupby('player_id')['transaction_type'].shift(-1)
history['next_next_transaction_type'] = history.groupby('player_id')['transaction_type'].shift(-2)
history['next_amount'] = history.groupby('player_id')['amount'].shift(-1)
history['next_next_payment_method_name'] = history.groupby('player_id')['payment_method_name'].shift(-2)
history['next_date'] = history.groupby('player_id')['Date'].shift(-1)
history['next_next_date'] = history.groupby('player_id')['Date'].shift(-2)



history['bet/deposit'] = round(history['next_amount'] / history['amount'] * 100, 0)

history['withwrad-deposit-time'] = (history['next_next_date'] - history['next_date']).fillna('-').apply(to_seconds)

# Just for myself, I kept data in Excel and in Excel figured out that there is no any user appropriate to the Task 1 requirements
# But anyway , let's move on with Python

# history.to_excel('result1.xlsx')

history_1 = history[(history['transaction_type'] == 'deposit') & 
                    (history['next_transaction_type'] == 'bet') &
                    (history['bet/deposit'] >= 90) &
                    (history['bet/deposit'] <= 110) &
                    (history['withwrad-deposit-time'] <= 3600) &
                    (history['payment_method_name'] != history['next_next_payment_method_name'])]

history_1 = history_1[['player_id']]

history_1.to_csv(r'result\resultSSMMDDMMYYYY.csv')


# TASK 2

# 2.1) на основании данных в папке bets, находит клиента, который сделал 5 выигрышных ставок подряд с коеф. > 1.5;
# 2.2) сохранить результат в папке result с названием "bets_resultSSMMDDMMYYYY.csv".


bets.reset_index(drop=True, inplace=True)

history['player_id'] = history['player_id'].astype(str)
history = history[history['player_id'].str.isnumeric()]
history['player_id'] = history['player_id'].astype(int)

bets = bets.sort_values(by=['player_id', 'Date'], ascending=[True, True])
bets['coefficient'] = bets['payout'] / bets['amount']




bets['2_result'] = bets.groupby('player_id')['result'].shift(-1)
bets['2_coefficient'] = bets.groupby('player_id')['coefficient'].shift(-1)

bets['3_result'] = bets.groupby('player_id')['result'].shift(-2)
bets['3_coefficient'] = bets.groupby('player_id')['coefficient'].shift(-2)

bets['4_result'] = bets.groupby('player_id')['result'].shift(-3)
bets['4_coefficient'] = bets.groupby('player_id')['coefficient'].shift(-3)

bets['5_result'] = bets.groupby('player_id')['result'].shift(-4)
bets['5_coefficient'] = bets.groupby('player_id')['coefficient'].shift(-4)


# bets.to_excel('bets.xlsx')

bets_2 = bets[(bets['result'] == 'Win') & (bets['coefficient'] > 1.5) &
                (bets['2_result'] == 'Win') & (bets['2_coefficient'] > 1.5) &
                (bets['3_result'] == 'Win') & (bets['3_coefficient'] > 1.5) &
                (bets['4_result'] == 'Win') & (bets['4_coefficient'] > 1.5) &
                (bets['5_result'] == 'Win') & (bets['5_coefficient'] > 1.5)]


bets_2 = bets_2[['player_id']].drop_duplicates()
bets_2.reset_index(drop=True, inplace=True)


bets_2.to_csv(r'result\bets_resultSSMMDDMMYYYY.csv')










