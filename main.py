
# -*- coding: utf-8 -*-
# --- LINH PHAN ---
from gearman import GearmanClient
import json
import csv
import sys
import mysql.connector
import time
from multiprocessing import Pool

# FOR ANALYSIS ADDRESS
GEARMAN_API         = "****"
FUNCTION            = "****"
FILE_ANALYSIS_OUT   = "****"

# FOR MULTIPROCESSING
MULTI_THREAD        = 20

# FOR DATABASE
DB_HOST             = "****"
USERNAME            = "linhph"
PASSWORD            = "****"
DB_NAME             = "****"
QUERY_GETDB         = "Select * from analysis_addresses"

reload(sys)
sys.setdefaultencoding('utf-8')

def analysis_address(row):
    out_row = []

    input_id = []
    input_addr = row[1].encode("utf-8")
    api_output_id = []
    api_output_addr = []

    client = GearmanClient([GEARMAN_API])
    job_request = client.submit_job(FUNCTION, input_addr)
    
    loaded_json = json.loads(job_request.result)
    for addr in loaded_json:
        api_output_id.append(addr['id'])
        api_output_addr.append(addr['string'])
    if(row[6] != ''):
        input_id = row[6].strip().split(" ")
        input_id = list(map(int, input_id))
    compare_list = set(input_id) ^ set(api_output_id)
    different = len(compare_list)
    str_api_output_addr = ', '.join(api_output_addr)
    str_api_output_id = ' '.join(str(e) for e in api_output_id)
    out_row = [row[0], row[1], row[2], str_api_output_addr, row[6], str_api_output_id, different]

    return out_row


if __name__ == '__main__':

    start_time = time.time()

    # Connect database:
    cnx = mysql.connector.connect(host=DB_HOST, user=USERNAME, passwd=PASSWORD, db=DB_NAME)

    cursor = cnx.cursor()
    cursor.execute(QUERY_GETDB)
    result = cursor.fetchall()

    p = Pool(MULTI_THREAD)
    csv_data = p.map(analysis_address, result)
    csv_data.insert(0,['Id', 'Input Address', 'Expected Output', 'API Output', 'Id Input', 'API Id Output', 'Different'])
    cnx.close()

    with open(FILE_ANALYSIS_OUT, 'wb') as csvFile:
        writer = csv.writer(csvFile)
        writer.writerows(csv_data)

    csvFile.close()

    elapsed_time = time.time() - start_time
    print("\n------ Elapsed time: " + str(round(elapsed_time, 3)) + "s ------")
