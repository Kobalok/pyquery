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
        
        with open('4pyquery.csv', 'r', encoding='utf-8') as csvfile:
            spamreader = csv.reader(csvfile, delimiter=';')
            for row in spamreader:
                mserver=row[0]#+SERVER 
                mpn=row[1]
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
                    SQL_QUERY = f"""USE {mdb};
                                        SET QUOTED_IDENTIFIER ON;
                    if object_id('tempdb..#temp') is not null  drop table #temp;
                    select distinct k.ID_KIZ_GLOBAL, ki.ID_KIZ_ITEM_GLOBAL, km.REQUEST_ID, km.DOCUMENT_OUT_ID,km.ID_OPERATION, ki.ID_DOCUMENT 
                    into #temp
                    from kiz k
                    join kiz_item ki on k.id_kiz_global=ki.id_kiz_global
                    join kiz_move km on ki.id_kiz_item_global=km.id_kiz_item_global
                    join KIZ_2_DOCUMENT_ITEM k2 on k.ID_KIZ_GLOBAL=k2.ID_KIZ_GLOBAL
                    join LOT l on k2.ID_DOCUMENT_ITEM_ADD=l.ID_DOCUMENT_ITEM_ADD
                    where (k.state='HOLD' or k.state='PROC' or k.state='EXIT') and  km.ID_ERROR in(-2,-1,0,11)
                    and l.QUANTITY_REM+ l.QUANTITY_RES>0
                    --and km.STATE='OK1'
                    --and k2.DATE_MODIFIED<getdate()-0.083
                    and l.INVOICE_NUM in(
                    --'52263/ПН-00011530',
                    {mpn}
                    );
                    if (select  count(*)  from  #temp)=0 return;
                    DECLARE @KIZ1 as uniqueidentifier;
                    DECLARE @KIZ2 as uniqueidentifier;
                    DECLARE @KIZ3 as uniqueidentifier;
                    DECLARE @KIZ4 as uniqueidentifier;
                    DECLARE @KIZ5 as uniqueidentifier;
                    DECLARE @KIZ6 as uniqueidentifier;

                    DECLARE db_cursor CURSOR FOR
                    select  ID_KIZ_GLOBAL, ID_KIZ_ITEM_GLOBAL, REQUEST_ID, DOCUMENT_OUT_ID, ID_OPERATION, ID_DOCUMENT   from  #temp;
                    OPEN db_cursor


                    FETCH NEXT FROM db_cursor INTO @KIZ1, @KIZ2, @KIZ3, @KIZ4, @KIZ5, @KIZ6 ; 
                    WHILE (@@FETCH_STATUS <> -1) BEGIN
                           
                            update KIZ
                                set STATE='PROC'
                            WHERE ID_KIZ_GLOBAL=@KIZ1 AND STATE='HOLD';

                            update KIZ_ITEM
                                set IS_READY=1
                            WHERE ID_KIZ_ITEM_GLOBAL=@KIZ2 AND IS_READY=0;
                    INSERT INTO KIZ_MOVE
                            (ID_KIZ_MOVE_GLOBAL
                            ,ID_KIZ_ITEM_GLOBAL
                            ,REQUEST_ID
                            ,DOCUMENT_OUT_ID
                            ,OP_DATE
                            ,OP_TYPE
                            ,ID_ERROR
                            ,ERROR
                            ,STATE
                            ,ID_STATE
                            ,ID_OPERATION)
                        VALUES
                            (newid()
                            ,@KIZ2
                            ,@KIZ3
                            ,@KIZ4
                            ,getdate()
                            ,'InBwReport'
                            ,0
                            ,''
                            ,'OK'
                            ,7
                            ,@KIZ5);
                    
                    FETCH NEXT FROM db_cursor  
                    INTO @KIZ1, @KIZ2, @KIZ3, @KIZ4, @KIZ5, @KIZ6;

                    END;
                    CLOSE db_cursor;
                    DEALLOCATE db_cursor;
                     """
                    cursor = conn.cursor()
                    cursor.execute(SQL_QUERY)
                    cursor.commit()
                    #while cursor.nextset():   # NB: This always skips the first resultset
                        #try:
                            #results = cursor.fetchall()
                            #break
                        #except pyodbc.ProgrammingError:
                            #continue
                    spamwriter.writerow([row,"Запрос выполнен без ошибок!!"])
                except Exception as err:
                    spamwriter.writerow([row,"Ошибка выполнения запроса:",err,type(err)])
                    print(f"Unexpected {err=}, {type(err)=}")
                finally:
                     cursor.close()
                     conn.close()