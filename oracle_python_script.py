# Import libraries
from datetime import datetime, timedelta
import cx_Oracle
import pandas as pd
import os
import time
from pathlib import Path

def main():
    # Connect to Oracle database
    def connect_to_oracle(host:str, port:str, service_name:str, user:str, password:str):
        try:
            dsn_tns = cx_Oracle.makedsn(host, port, service_name=service_name)
            connection = cx_Oracle.connect(user=user, password=password, dsn=dsn_tns)
            conn = connection.cursor()
            return conn

        except Exception as e:
                print(f'failed to connect to oracle database due to the following error: {e}')

    # First Stage: Create First Table
    def create_first_table(conn, yesterday_date):
        try:
            conn.execute(f'''
                      create table TEMP_STAMPWKY_NG01_STG1_DLY_yesterday_{yesterday_date} AS 
                      SELECT t2.tran_id CR_TRAN_ID,
                      t2.PART_TRAN_SRL_NUM CR_PART_TRAN_SRL_NUM,
                      t2.tran_date CR_TRAN_DATE,
                      t2.value_date CR_VALUE_DATE,
                      t2.PSTD_DATE CR_PSTD_DATE,
                      t2.RCRE_TIME CR_RCRE_TIME,
                      t2.tran_amt CR_TRAN_AMT,
                      t1.acid CR_ACID,
                      t1.foracid CR_ACCOUNT,
                      t1.acct_name CR_ACCOUNT_NAME,
                      t2.tran_particular CR_TRAN_PARTICULAR,
                      t3.tran_id DR_TRAN_ID,
                      t3.PART_TRAN_SRL_NUM DR_PART_TRAN_SRL_NUM,
                      t3.tran_date DR_TRAN_DATE,
                      t3.value_date DR_VALUE_DATE,
                      t3.PSTD_DATE DR_PSTD_DATE,
                      t3.RCRE_TIME DR_RCRE_TIME,
                      t3.tran_amt DR_TRAN_AMT,
                      t4.acid DR_ACID,
                      t4.SCHM_TYPE ACCOUNT_TYPE,
                      t4.CIF_ID DR_CUSTOMER_ID,
                      t4.foracid DR_ACCOUNT,
                      t4.acct_name DR_ACCOUNT_NAME,
                      t3.tran_particular DR_TRAN_PARTICULAR
                 FROM TBAADM.GAM@FINACLE_PRD_NG t1,
                      TBAADM.HTD@FINACLE_PRD_NG t2,
                      TBAADM.HTD@FINACLE_PRD_NG t3,
                      TBAADM.GAM@FINACLE_PRD_NG t4
                WHERE t1.acid = t2.acid
                      AND t2.tran_date = {yesterday_date}
                      AND t2.part_tran_type = 'C'
                      AND t2.PSTD_FLG = 'Y'
                      AND t1.foracid LIKE 'NGN%2508080'
                      AND t2.tran_date = t3.tran_date
                      AND t2.tran_id = t3.tran_id
                      AND t2.tran_amt = t3.tran_amt
                      AND t2.PART_TRAN_SRL_NUM != t3.PART_TRAN_SRL_NUM
                      AND t3.part_tran_type = 'D'
                      AND t3.PSTD_FLG = 'Y'
                      AND t3.acid = t4.acid
                      ''')
            print('Table successfully completed')
        
        except Exception as e:
            print(f'first table did not create due to the following error: {e}')

    # Second Stage: Create Second Table
    def create_second_table(conn, yesterday_date):
        try:
            conn.execute(f'''create table TEMP_STAMPWKY_NG01_STG2_DLY_yesterday_{yesterday_date} AS 
                        SELECT DISTINCT t1.foracid,
                               T1.TRAN_ID,
                               TRUNC (T1.TRAN_DATE) tran_date,
                               t1.PART_TRAN_SRL_NUM,
                               t2.dr_acid acid
                        FROM CUSTOM.STAMP_DUTY@FINACLE_DR_NG t1,
                        TEMP_STAMPWKY_NG01_STG1_DLY_yesterday_{yesterday_date} t2
                        WHERE   t1.foracid(+) = t2.DR_ACCOUNT
                        AND TRUNC (COll_DATE(+)) = TRUNC (DR_PSTD_DATE)
                        AND CHRG_TRAN_ID(+) = DR_TRAN_ID
                        AND coll_flg(+) = 'Y')
                        ''')
            print('second table successfully created')
        
        except Exception as e:
            print(f'second table did not create due to the following error: {e}')

    # Third Stage: Create Index
    def create_index_on_second_table(conn, yesterday_date):
        try:
            conn.execute(f'''create CREATE INDEX TEMP_STAMPKY_NG01_STG2_DLY_{yesterday_date} ON
                 TEMP_STAMPWKY_NG01_STG2_DLY_NOV19 (TRAN_DATE, TRAN_ID)
                 ''')
            print('index on second table successfully created')
        
        except Exception as e:
            print(f'second did not create due to the following error: {e}')


    def create_other_tables_and_insert(conn, yesterday_date):
        try:
            conn.execute(f'''
            with TEMP_STAMPWKY_NG01_STG3_DLY_{yesterday_date} AS
            (SELECT t2.*, t1.ACCOUNT_NO FORACID, t1.CUSTOMER_ID CIF_ID
             FROM VISION.ACCOUNTS_DLY t1,
            TBAADM.HTD@FINACLE_PRD_NG t2,
            (SELECT tran_id, tran_date FROM TEMP_STAMPKY_NG01_STG2_DLY_{yesterday_date}) t3
            WHERE     t1.COUNTRY = 'NG'
            AND LE_BOOK = '01'
            AND t1.country = t2.bank_id
            AND t1.INTERNAL_acid = t2.acid
            AND t2.tran_date = t3.tran_date
            AND t2.tran_id = t3.tran_id
            AND t2.pstd_flg = 'Y'
            AND T2.DEL_FLG != 'Y'),

            TEMP_STAMPWKY_NG01_STG4_DLY_{yesterday_date} AS
            (SELECT MAX (I2.FORACID) FORACID,BANK_ID,I2.TRAN_DATE,I2.TRAN_ID
            FROM TEMP_STAMPWKY_NG01_STG3_DLY_{yesterday_date} I2
            WHERE I2.PART_TRAN_TYPE = 'D'
            GROUP BY BANK_ID, I2.TRAN_DATE, I2.TRAN_ID),

            temp_stampwky_ng01_stg5_DLY_rerun_{yesterday_date} AS 
            (SELECT A1.*, A2.CUSTOMER_ID, A2.ACCOUNT_NAME,A3.BVN
            FROM TEMP_STAMPWKY_NG01_STG4_DLY_{yesterday_date} A1, vision.ACCOUNTS_DLY A2,
            (SELECT customer_id, MAX (BVN) BVN FROM vision.RPT_CUSTOMER_BVN
            WHERE BVN IS NOT NULL AND UPPER (BVN) != 'NULL'
            GROUP BY customer_id) A3
            WHERE A2.COUNTRY = 'NG'
            AND A2.LE_BOOK = '01'
            AND A2.ACCOUNT_NO = A1.FORACID
            AND A2.CUSTOMER_ID = A3.CUSTOMER_ID(+)),

            TEMP_STAMPWKY_NG01_STG6_DLY_{yesterday_date} AS 
            (SELECT H2.ACCOUNT_NO RECEIVING_BANK_ACCOUNT_NUMBER,H2.ACCOUNT_NAME RECEIVING_BANK_ACCOUNT_NAME,H3.BVN RECEIVING_BVN,
            CASE WHEN H4.CUSTOMER_ID <> 'D000' THEN H4.FORACID END SENDER_ACCOUNT_NUMBER, H4.ACCOUNT_NAME SENDER_ACCOUNT_NAME,
            CASE WHEN H4.CUSTOMER_ID <> 'D000' THEN H4.BVN END SENDER_BVN,
            CASE WHEN H1.PSTD_USER_ID = 'CDCI' AND (UPPER (H1.TRAN_PARTICULAR) LIKE 'WEB%' OR UPPER (H1.TRAN_PARTICULAR) LIKE 'POS%') 
            THEN 1
            WHEN H1.PSTD_USER_ID = 'CDCI' THEN 3 ELSE 2 END PAYMENT_TYPE, H1.TRAN_DATE DATE_OF_TRANSACTION, H1.TRAN_AMT AMOUNT,
            CASE WHEN H2.CURRENCY <> 'NGN' THEN 3 ELSE DECODE (H2.ACCOUNT_TYPE,  'CAA', 1,  'ODA', 1,  'SBA', 2) END ACCOUNT_TYPE,
            3 ACCOUNT_CLASS, H1.BANK_ID COUNTRY, '01' LE_BOOK, H1.TRAN_ID, H1.PART_TRAN_SRL_NUM
                FROM TEMP_STAMPWKY_NG01_STG3_DLY_{yesterday_date} H1, vision.ACCOUNTS_DLY H2,
                (SELECT CUSTOMER_ID, MAX (BVN) BVN FROM vision.RPT_CUSTOMER_BVN WHERE BVN IS NOT NULL OR UPPER (BVN) != 'NULL'
                GROUP BY CUSTOMER_ID) H3,
                temp_stampwky_ng01_stg5_DLY_rerun_{yesterday_date} H4, temp_stampwky_ng01_stg2_DLY_{yesterday_date} H5
                WHERE H1.BANK_ID = H2.COUNTRY
                AND H1.FORACID = H2.ACCOUNT_NO
                AND H2.CUSTOMER_ID = H3.CUSTOMER_ID(+)
                AND H1.BANK_ID = H4.BANK_ID
                AND H1.TRAN_DATE = H4.TRAN_DATE
                AND H1.TRAN_ID = H4.TRAN_ID
                AND H2.COUNTRY = 'NG'
                AND H2.LE_BOOK = '01'
                AND H2.ACCOUNT_TYPE IN ('CAA', 'ODA', 'SBA')
                AND H1.PART_TRAN_TYPE = 'C'
                AND H1.FORACID = h5.foracid
                AND H1.TRAN_DATE = H5.TRAN_DATE
                AND H1.TRAN_ID = H5.TRAN_ID
                AND h1.PART_TRAN_SRL_NUM = h5.PART_TRAN_SRL_NUM)

            INSERT INTO rpt_stampduty_dly (week_date,receiving_bank_account_number,receiving_bank_account_name,receiving_bvn,
                                           sender_account_number,sender_account_name,sender_bvn,payment_type,date_of_transaction,
                                           amount,account_type,account_class,country,le_book,tran_id,part_tran_srl_num)
            SELECT DISTINCT '19-NOV-2023' week_date, t1.* FROM TEMP_STAMPWKY_NG01_STG6_DLY_{yesterday_date} t1
            ''')

            conn.execute('commit')
            print('third , fourth, fifth and sixth tables successfully created. First INSERT completed too')

        except Exception as e:
            print(f'third, fourth, fifth and sixth tables not created due to the following error: {e}')

    def execute_retieve_emtl_report(conn, yesterday_date, file_path):
        try:
            result = conn.execute(f'''
                    SELECT {yesterday_date} datee,NVL (j.State, 'NA') State,
                             COUNT(CASE WHEN ac.corp_id IS NOT NULL THEN k.Receiving_Bank_Account_Number END) Corporate,
                             COUNT(CASE WHEN ac.corp_id IS NULL THEN k.Receiving_Bank_Account_Number END) Individual,
                             COUNT (*) Transaction_Count,
                             COUNT (*) * 50 Cumulative_Amount_charged
                        FROM BIUSER.rpt_stampduty_dly k, vision.accounts_dly dl, vision.PWT_WHT_STATES j, crmuser.accounts@finacle_Dr_ng ac
                       WHERE k.Receiving_Bank_Account_Number = dl.Account_No
                             AND dl.country = 'NG'
                             AND dl.le_book = '01'
                             AND SUBSTR (dl.Vision_Ouc, 7, 4) = j.Sol_Id(+)
                             AND dl.Customer_Id = ac.orgkey
                             AND dl.Country = ac.bank_id
                             AND Week_Date = {yesterday_date}
                    GROUP BY NVL (j.State, 'NA')
                                 ''')
            # Convert the result to a Pandas DataFrame
            df = pd.DataFrame(result.fetchall(), columns=result.keys())

            # Save the DataFrame to a CSV file
            df.to_csv(file_path, index=False)

            print(f"CSV containing output successfully saved at: {file_path}")

        except Exception as e:
            print(f'final output failed to download due to the following error: {e}')

    # Execute Second INSERT
    def second_insert(conn, yesterday_date):
        try:
            conn.execute(f'''
            INSERT INTO rpt_stamp_sum_txn_dt_dly (category, count_txn, volume_txn, date_of_transaction, week_date)
            SELECT category, COUNT (*) count_txn, COUNT (*) * 50 volume_txn, date_of_transaction,{yesterday_date} week_date
            FROM (SELECT DISTINCT h2.account_no receiving_bank_account_number, h2.account_name receiving_bank_account_name,
                    h3.bvn receiving_bvn, CASE WHEN h4.customer_id <> 'D000' THEN h4.foracid END sender_account_number,
                    h4.account_name sender_account_name, CASE WHEN h4.customer_id <> 'D000' THEN h4.bvn END sender_bvn,
                    CASE WHEN h1.pstd_user_id = 'CDCI' 
                    AND (UPPER(h1.tran_particular) LIKE 'WEB%' OR UPPER (h1.tran_particular) LIKE 'POS%') THEN 1
                    WHEN h1.pstd_user_id = 'CDCI' THEN 3 ELSE 2 END payment_type,
                    h1.tran_date date_of_transaction, h1.tran_amt amount,
                    CASE WHEN h2.currency <> 'NGN' THEN 3 ELSE DECODE (h2.account_type, 'CAA',1,'ODA', 1,'SBA', 2) END account_type,
                        3 account_class, h1.bank_id country, '01' le_book, h1.tran_id, h1.part_tran_srl_num,
                    CASE WHEN tran_type = 'C' THEN 'CASH'
                         WHEN h1.pstd_user_id = 'CDCI' AND UPPER (h1.tran_particular) LIKE 'WEB%' THEN 'WEB'
                         WHEN h1.pstd_user_id = 'CDCI' AND UPPER (h1.tran_particular) LIKE 'POS%' THEN 'WEB'
                         WHEN source1 = 'INTERBANK' THEN 'INTERBANK' ELSE 'INTRABANK' END category
                    FROM temp_stampwky_ng01_stg3_DLY_NOV19 h1, 
                    vision.accounts_dly h2,
                     (SELECT customer_id, MAX (bvn) bvn FROM vision.rpt_customer_bvn 
                     WHERE bvn IS NOT NULL OR UPPER (bvn) != 'NULL'
                     GROUP BY customer_id) h3,
                    temp_stampwky_ng01_stg5_DLY_rerun_NOV19 h4,
                    temp_stampwky_ng01_stg2_DLY_NOV19 h5,
                    (SELECT * 
                    FROM (SELECT tran_date, tran_id, bank_id, 
                                 ROW_NUMBER () OVER (PARTITION BY tran_date, tran_id, bank_id ORDER BY source1 ASC) rnk,
                                 source1
                          FROM (SELECT DISTINCT tran_date, tran_id, bank_id,
                                    CASE WHEN foracid LIKE 'NGN%2509558' OR foracid LIKE 'USD%1220433' OR foracid LIKE 'USD%1220434'
                                    OR foracid LIKE 'GBP%1220434' OR foracid LIKE 'GBP%1220432' OR foracid LIKE 'EUR%1220435'
                                    OR foracid LIKE 'EUR%1220432' OR foracid LIKE 'NGN%2528070' OR foracid LIKE 'NGN%2528071'
                                    OR foracid LIKE 'NGN%2527084' OR foracid LIKE 'NGN%2508303' OR foracid LIKE 'NGN%1210001'
                                    OR foracid LIKE 'NGN%2509558' OR foracid LIKE 'NGN%2508058' OR foracid LIKE 'NGN%2508065'
                                    OR foracid LIKE 'NGN%1931114' OR foracid LIKE 'NGN%1211001' OR foracid LIKE 'NGN%1614059'
                                    OR foracid LIKE 'NGN%1931151' THEN 'INTERBANK' ELSE 'OTHERS' END source1
                                FROM temp_stampwky_ng01_stg3_DLY_{yesterday_date} WHERE part_tran_type = 'D'))
                          WHERE rnk = 1) h6
                    WHERE  h1.bank_id = h2.country AND h1.foracid = h2.account_no AND h2.customer_id = h3.customer_id(+)
                           AND h1.bank_id = h4.bank_id AND h1.tran_date = h4.tran_date AND h1.tran_id = h4.tran_id
                           AND h2.country = 'NG' AND h2.le_book = '01' AND h2.account_type IN ('CAA', 'ODA', 'SBA')
                           AND h1.part_tran_type = 'C' AND h1.foracid = h5.foracid AND h1.tran_date = h5.tran_date
                           AND h1.tran_id = h5.tran_id AND h1.part_tran_srl_num = h5.part_tran_srl_num 
                           AND h1.bank_id = h6.bank_id
                           AND h1.tran_date = h6.tran_date AND h1.tran_id = h6.tran_id)
            GROUP BY category, date_of_transaction
            ''')
            conn.execute('commit')
            print('second INSERT completed too')
        except Exception as e:
            print(f'second INSERT failed due to the following error: {e}')

    def third_insert(conn, yesterday_date):
        try:
            conn.execute(f'''
            INSERT INTO rpt_stamp_summary_dly (category, count_txn, volume_txn, week_date)

            SELECT category, COUNT (*) count_txn, COUNT (*) * 50 volume_txn, {yesterday_date} week_date
            FROM (SELECT DISTINCT h2.account_no receiving_bank_account_number, h2.account_name receiving_bank_account_name,
                    h3.bvn receiving_bvn, CASE WHEN h4.customer_id <> 'D000' THEN h4.foracid END sender_account_number,
                    h4.account_name sender_account_name, CASE WHEN h4.customer_id <> 'D000' THEN h4.bvn END sender_bvn,
                    CASE WHEN h1.pstd_user_id = 'CDCI'
                    AND (UPPER (h1.tran_particular) LIKE 'WEB%' OR UPPER(h1.tran_particular) LIKE 'POS%') THEN 1
                    WHEN h1.pstd_user_id = 'CDCI' THEN 3 ELSE 2 END payment_type, h1.tran_date date_of_transaction, h1.tran_amt amount,
                    CASE WHEN h2.currency <> 'NGN' THEN 3 ELSE DECODE (h2.account_type,'CAA', 1,'ODA', 1,'SBA', 2) END account_type,
                    3 account_class, h1.bank_id country, '01' le_book, h1.tran_id, h1.part_tran_srl_num,
                    CASE WHEN tran_type = 'C' THEN 'CASH' 
                         WHEN h1.pstd_user_id = 'CDCI' AND UPPER (h1.tran_particular) LIKE 'WEB%' THEN 'WEB'
                         WHEN h1.pstd_user_id = 'CDCI' AND UPPER (h1.tran_particular) LIKE 'POS%' THEN 'WEB' 
                         WHEN source1 = 'INTERBANK' THEN 'INTERBANK' ELSE 'INTRABANK' END category
                    FROM temp_stampwky_ng01_stg3_DLY_{yesterday_date} h1, vision.accounts_dly h2,
                    (SELECT customer_id, MAX (bvn) bvn FROM vision.rpt_customer_bvn 
                    WHERE bvn IS NOT NULL OR UPPER (bvn) != 'NULL' GROUP BY customer_id) h3,
                    temp_stampwky_ng01_stg5_DLY_rerun_NOV19 h4, temp_stampwky_ng01_stg2_DLY_NOV19 h5,
                    (SELECT *
                        FROM (SELECT tran_date, tran_id, bank_id, 
                                    ROW_NUMBER () OVER (PARTITION BY tran_date, tran_id, bank_id ORDER BY source1 ASC) rnk, source1
                                    FROM (SELECT DISTINCT tran_date, tran_id, bank_id,
                                                 CASE WHEN foracid LIKE 'NGN%2509558' OR foracid LIKE 'USD%1220433'
                                                 OR foracid LIKE 'USD%1220434' OR foracid LIKE 'GBP%1220434' OR foracid LIKE 'GBP%1220432' 
                                                 OR foracid LIKE 'EUR%1220435' OR foracid LIKE 'EUR%1220432' OR foracid LIKE 'NGN%2528070' 
                                                 OR foracid LIKE 'NGN%2528071' OR foracid LIKE 'NGN%2527084' OR foracid LIKE 'NGN%2508303'
                                                 OR foracid LIKE 'NGN%1210001' OR foracid LIKE 'NGN%2509558' OR foracid LIKE 'NGN%2508058'
                                                 OR foracid LIKE 'NGN%2508065' OR foracid LIKE 'NGN%1931114' OR foracid LIKE 'NGN%1211001'
                                                 OR foracid LIKE 'NGN%1614059' OR foracid LIKE 'NGN%1931151' THEN 'INTERBANK' ELSE
                                                 'OTHERS' END source1
                                            FROM temp_stampwky_ng01_stg3_DLY_NOV19
                                                WHERE part_tran_type = 'D'))
                                        WHERE rnk = 1) h6
                               WHERE  h1.bank_id = h2.country AND h1.foracid = h2.account_no AND h2.customer_id = h3.customer_id(+)
                               AND h1.bank_id = h4.bank_id AND h1.tran_date = h4.tran_date AND h1.tran_id = h4.tran_id
                               AND h2.country = 'NG' AND h2.le_book = '01' AND h2.account_type IN ('CAA', 'ODA', 'SBA')
                               AND h1.part_tran_type = 'C' AND h1.foracid = h5.foracid AND h1.tran_date = h5.tran_date
                               AND h1.tran_id = h5.tran_id AND h1.part_tran_srl_num = h5.part_tran_srl_num 
                               AND h1.bank_id = h6.bank_id AND h1.tran_date = h6.tran_date AND h1.tran_id = h6.tran_id)
                   GROUP BY category
               UNION ALL

               SELECT * 
               FROM (SELECT 'POS_MERCHANT' category, SUM (tran_amt) / 50 count_txn, SUM (tran_amt) volume_txn,
                            '19-NOV-2023' week_date
                            FROM (SELECT TO_DATE(SUBSTR (tran_particular2, INSTR (tran_particular2,'-',1,2) + 1, 4)
                                 || LPAD (SUBSTR (tran_particular2, INSTR (tran_particular2, '-') + 1, INSTR (tran_particular2,
                                    '-', 1, 2) - INSTR (tran_particular2, '-') + 1), 2, 0) || LPAD (SUBSTR (
                                                  tran_particular2, 13, INSTR (tran_particular2, '-') - 13), 2, 0), 'YYYYMMDD') settlemnt_date,
                                      cred_leg.*
                                 FROM (SELECT REPLACE (REPLACE (tran_particular, '/', ''),'-019','-2019') tran_particular2, a.*
                                         FROM tbaadm.htd@finacle_prd_ng a) cred_leg
                                        WHERE  tran_date >= '18sep2019' 
                                        AND cred_leg.acid IN (SELECT acid FROM tbaadm.gam@finacle_prd_ng WHERE foracid = '1022334931')
                                      AND tran_sub_type = 'BI' AND pstd_flg = 'Y' AND tran_particular LIKE 'STAMP DUTY POS:%'
                                      AND part_tran_type = 'C')
                        WHERE tran_date = {yesterday_date}
                        UNION ALL
                        SELECT 'WEB_MERCHANT' category, SUM (tran_amt) / 50 count_txn,  SUM (tran_amt) volume_txn,
                        '19-NOV-2023' week_date
                            FROM (SELECT TO_DATE (SUBSTR (tran_particular2, INSTR (tran_particular2,'-',1,2)+ 1, 4)
                                || LPAD (SUBSTR (tran_particular2, INSTR (tran_particular2, '-') + 1, INSTR (tran_particular2,
                                   '-', 1, 2) - INSTR (tran_particular2, '-') + 1), 2, 0) || LPAD (SUBSTR (tran_particular2,
                                    17, INSTR (tran_particular2, '-') - 17), 2, 0), 'YYYYMMDD') settlemnt_date,
                                      cred_leg.*
                                 FROM (SELECT REPLACE(REPLACE (tran_particular, '/', ''),'-019', '-2019') tran_particular2, a.*
                                         FROM tbaadm.htd@finacle_prd_ng a) cred_leg WHERE tran_date >= '18sep2019'
                                         AND cred_leg.acid IN (SELECT acid FROM tbaadm.gam@finacle_prd_ng WHERE foracid = '1022334931')
                                         AND tran_sub_type = 'BI' AND pstd_flg = 'Y' AND tran_particular LIKE 'STAMP DUTY WEB%'
                                         AND part_tran_type = 'C')
                                         WHERE tran_date = {yesterday_date})
            ''')
            conn.execute('COMMIT')
            print('third INSERT successfully completed')

        except Exception as e:
            print(f'third INSERT failed due to the following error: {e}')

    # Calling the functions
    # Connect to oracle database
    host = ''
    port = ''
    service_name = ''
    user = ''
    password = ''
	
    conn = connect_to_oracle(host, port, service_name, user, password)

    # Estimate yesterday's date in DD-MON-YYYY
    yesterday_datetime = datetime.now() - timedelta(days=1)
    yesterday_date = yesterday_datetime.strftime('%d-%b-%Y').upper()

    # File path to save report
    file_path = Path('')

    # Execute SQL queries
    create_first_table(conn, yesterday_date)
    create_second_table(conn, yesterday_date)
    create_index_on_second_table(conn, yesterday_date)
    create_other_tables_and_insert(conn, yesterday_date)
    execute_retieve_emtl_report(conn, yesterday_date, file_path)
    second_insert(conn, yesterday_date)
    third_insert(conn, yesterday_date)


if __name__ == '__main__':
    main()