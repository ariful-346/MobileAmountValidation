import MySQLdb
import time
import sys
from math import ceil
from multiprocessing import Process
import json
import csv

config = {}


def amount_validation(row_data):
    if not row_data['amount'] or not row_data['amount'].strip():
        row_data['is_amount_absent'] = True
        return True

    try:
        amount = int(row_data['amount'].strip())

    except ValueError:

        row_data['is_amount_invalid'] = True
        return True

    if amount == 0:

        row_data['is_amount_absent'] = True

    elif 0 < amount < 50 or amount % 25 != 0:

        row_data['is_amount_invalid'] = True

    elif row_data['no_of_student'] == 1 and amount > 375:

        row_data['is_amount_invalid'] = True

    elif row_data['no_of_student'] == 2 and amount > 750:

        row_data['is_amount_invalid'] = True

    elif row_data['no_of_student'] == 3 and amount > 1050:

        row_data['is_amount_invalid'] = True

    elif row_data['no_of_student'] == 4 and amount > 1350:

        row_data['is_amount_invalid'] = True

    elif row_data['no_of_student'] not in config["number_of_student_list"] and amount > 1350 and row_data[
        'no_of_student'] is not None:

        row_data['is_amount_invalid'] = True

    else:
        row_data['is_amount_absent'] = False
        row_data['is_amount_invalid'] = False

        return False

    return True


def mobile_validation(row_data):
    if not row_data['mobile_no'] or not row_data['mobile_no'].strip() or len(row_data['mobile_no']) <= 8:

        row_data['is_mobile_absent'] = True

    elif len(row_data['mobile_no']) > 8 and len(row_data['mobile_no']) not in config["valid_mobile_length_list"]:
        row_data['is_mobile_char_count_invalid'] = True

    elif len(row_data['mobile_no']) == 11 and row_data['mobile_no'][:3] not in config["mobile_operator_list"]:
        row_data['is_mobile_operator_invalid'] = True

    else:
        row_data['is_mobile_absent'] = False
        row_data['is_mobile_char_count_invalid'] = False
        row_data['is_mobile_operator_invalid'] = False

        return False

    return True


def dup_mobile_in_same_school_validation(school_data):
    for i in range(0, (len(school_data)-1)):

        if not school_data[i]['is_mobile_absent'] and not school_data[i][
            'is_mobile_char_count_invalid'] and not school_data[i]['is_mobile_operator_invalid']:

            if school_data[i]['mobile_no'] == school_data[i + 1]['mobile_no']:
                school_data[i]['is_mobile_duplicate_same_school'] = True
                school_data[i + 1]['is_mobile_duplicate_same_school'] = True
                school_data[i]['status'] = 4
                school_data[i + 1]['status'] = 4


def validation(school_codes, process_name):
    global config

    db_conn = MySQLdb.connect(config['ip'], config['username'], config['password'], config['database'])

    for scl_code in school_codes:

        cursor = db_conn.cursor(MySQLdb.cursors.DictCursor)

        cursor.execute("""select id,school_code, amount, no_of_student, 
                    if (is_amount_absent = 1, TRUE , FALSE ) as is_amount_absent, 
                    if (is_amount_invalid = 1, TRUE , FALSE ) as is_amount_invalid, 
                    mobile_no,
                    if (is_mobile_absent = 1, TRUE , FALSE ) as is_mobile_absent, 
                    if (is_mobile_char_count_invalid = 1, TRUE , FALSE ) as is_mobile_char_count_invalid, 
                    if (is_mobile_operator_invalid = 1, TRUE , FALSE ) as is_mobile_operator_invalid, 
                    if (is_mobile_duplicate_same_school = 1, TRUE , FALSE ) as is_mobile_duplicate_same_school,
                    `status` 
                    from %s 
                    where school_code = '%s' and `status` = 0  
                    order by mobile_no """ % (config['table'], scl_code))

        school_data = cursor.fetchall()
        cursor.close()

        for data in school_data:
            ret_val_amount_validation = amount_validation(data)
            ret_val_mobile_validation = mobile_validation(data)

            if ret_val_amount_validation or ret_val_mobile_validation:
                data['status'] = 4
            else:
                data['status'] = 1

        dup_mobile_in_same_school_validation(school_data)

        tpl = []

        for data in school_data:
            tpl.append((data['is_amount_absent'], data['is_amount_invalid']
                        , data['is_mobile_absent'], data['is_mobile_char_count_invalid']
                        , data['is_mobile_operator_invalid'], data['is_mobile_duplicate_same_school']
                        , data['status'], data['id']))
        update_db(db_conn, tpl)

    db_conn.close()


def update_db(db_conn, tpl):
    cursor = db_conn.cursor()

    sql = "update `%s`" % config['table']
    sql += """set is_amount_absent = %s,
            is_amount_invalid = %s,
            is_mobile_absent = %s,
            is_mobile_char_count_invalid = %s,
            is_mobile_operator_invalid = %s,
            is_mobile_duplicate_same_school = %s,
            status = %s
            where id = %s"""

    cursor.executemany(sql, tpl)
    db_conn.commit()
    cursor.close()


def reset_data():
    global config

    db_conn = MySQLdb.connect(config['ip'], config['username'], config['password'], config['database'])
    cursor = db_conn.cursor()

    sql = """
            update `%s`
            set 
            is_amount_absent = false,
            is_amount_invalid = false,
            is_mobile_absent = false,
            is_mobile_char_count_invalid = false,
            is_mobile_duplicate_different_school = false,
            is_mobile_duplicate_same_school = false,
            is_mobile_duplicate_same_school = false,
            is_mobile_operator_invalid = false,
            `status` = 0
          """ % (config['table'])
    try:
        cursor.execute(sql)
        db_conn.commit()
        cursor.close()
        db_conn.close()

    except Exception as e:

        print e


def get_schools():
    school_list = []

    with open('input.csv', 'r') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=' ', quotechar='|')

        for row in spamreader:
            school_list.append(', '.join(row))

    return school_list


def get_schools_by_thana():
    thana_list = []

    with open('input.csv', 'r') as csvfile:

        spamreader = csv.reader(csvfile, delimiter=' ', quotechar='|')

        for row in spamreader:
            thana_list.append(', '.join(row))

    global config

    db_conn = MySQLdb.connect(config['ip'], config['username'], config['password'], config['database'])

    cursor = db_conn.cursor(MySQLdb.cursors.DictCursor)

    school_codes = []

    cursor.execute("""
                    select distinct rep.school_code from `%s` rep
                    inner join bugs.arif_school s
                    on rep.school_code = s.school_code
                    where s.thana_id in (%s)
                    """ % (config['table'], ' ,'.join(thana_list)))

    for item in cursor.fetchall():
        school_codes.append(item['school_code'])

    cursor.close()
    db_conn.close()

    return school_codes


def read_configuration():
    global config

    with open('config.json', 'r') as f:
        config = json.load(f)


if __name__ == '__main__':

    procs = []
    process_name = 'process-'

    print 'processing started....'
    start_time = time.time()

    read_configuration()
    reset_data()

    schools = []

    if config['by_school_code']:

        schools = get_schools()

    elif config['by_thana_id']:

        schools = get_schools_by_thana()

    number_of_process = 30

    ratio = int(ceil(len(schools) / number_of_process))

    for i in range(0, number_of_process):

        process_name += str(i)
        if i == (number_of_process - 1):

            proc = Process(target=validation,
                           args=(schools[i * ratio:], process_name))
        else:
            proc = Process(target=validation,
                           args=(schools[i * ratio:(i + 1) * ratio], process_name))

        procs.append(proc)
        proc.start()
        process_name = 'process-'

    for proc in procs:
        proc.join()

    print '%s time taken to complete.' % (time.time() - start_time)
