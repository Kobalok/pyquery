"""
Connects to a SQL database using pyodbc
and run sql query
"""
import pyodbc
import csv

SERVER = '.apt.rigla.ru\SQLEXPRESS,8703'
DATABASE = 'master'
USERNAME = 'sa'
PASSWORD = 'efarma2'

with open('out.csv', 'w', encoding='utf8') as csvout:
        spamwriter = csv.writer(csvout, delimiter=';')
        
        with open('in.csv', 'r', encoding='utf8') as csvfile:
            spamreader = csv.reader(csvfile, delimiter=';')
            for row in spamreader:
                mserver=row[0]+SERVER 
              
                connectionString = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={mserver};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}'
                try:
                    conn = pyodbc.connect(connectionString) 
                    SQL_QUERY = """
                    select name from sys.databases
                    """
                    cursor = conn.cursor()
                    cursor.execute(SQL_QUERY)
                    records = cursor.fetchall()
                    for r in records:
                        if r[0][:5].lower()=='rigla':
                                mdb=r[0]
                                break
                    cursor.close()
                    SQL_QUERY = f'USE {mdb};'
                    cursor = conn.cursor()
                    cursor.execute(SQL_QUERY)
                    cursor.close()
                    SQL_QUERY = """
                update KIZ_INVENTORY_ITEM_MDLP_CSV
set SOLUTION='Отправить 552', SCHEME=552, ID_CHEQUE=NULL
where ID_KIZ_INVENTORY_GLOBAL=(select top 1 ID_KIZ_INVENTORY_GLOBAL from KIZ_INVENTORY
order by DOC_DATE desc) and ID_CHEQUE is not NULL
and ID_CHEQUE in(select ID_CHEQUE from CHEQUE where DATE_CHEQUE<cast(cast(getdate() as date)as datetime)-29)

                     """
                    cursor = conn.cursor()
                    cursor.execute(SQL_QUERY)
                    while cursor.nextset():   # NB: This always skips the first resultset
                        try:
                            results = cursor.fetchall()
                            break
                        except pyodbc.ProgrammingError:
                            continue
                    spamwriter.writerow([row,"Запрос выполнен без ошибок!!"])
                except Exception as err:
                    spamwriter.writerow([row,"Ошибка выполнения запроса:",err,type(err)])
                    print(f"Unexpected {err=}, {type(err)=}")
                finally:
                     cursor.close()
                     conn.close()