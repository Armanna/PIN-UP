# PIN-UP


# How to start

1. git clone https://github.com/Armanna/PIN-UP.git
2. python -m venv .venv
3. .\.venv\Scripts\activate
4. pip install -r requirements
5. python script1.py






## PYTHON TASK

Данные:
- .сsv в папке payments
- .сsv в папке bets

Модель и описание данных отсутствует.

Необходимо написать скрипт с названием "script1.py", который в случае создания/удаления файла в папках payments и bets, запускал бы скрипт "script2.py". Здесь может быть несколько способов реализации и это остается "на усмотрение".

Скрипт "script2.py" должен выполнять следующие операции:
   1.1) на основании данных в папках payments и bets, находит клиента, который совершил такую последовательность операций:
       1.1.1) депозит;
       1.1.2) ставка на сумму депозита +-10%;
       1.1.3) вывод в течении часа от депозита через систему, отличную от депозита;
   1.2) сохранить результат в папке result с названием "resultSSMMDDMMYYYY.csv";
   2.1) на основании данных в папке bets, находит клиента, который сделал 5 выигрышных ставок подряд с коеф. > 1.5;
   2.2) сохранить результат в папке result с названием "bets_resultSSMMDDMMYYYY.csv".


## SQL TASK

https://platform.stratascratch.com/coding/9701-3rd-most-reported-health-issues

Solution -> sql_task.sql





