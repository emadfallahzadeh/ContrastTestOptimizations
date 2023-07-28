import psycopg2
import psycopg2.extras
import configparser
from datetime import timedelta

config_parser = configparser.RawConfigParser()
config_file_path = r'config.conf'
config_parser.read(config_file_path)
db_name = config_parser.get('General', 'db_name')
cpu_count = int(config_parser.get('General', 'cpu_count'))
main_batch_size = int(config_parser.get('General', 'batch_size')) # 2 4 0:BatchAll
max_batch_size = int(config_parser.get('General', 'max_batch_size'))

if main_batch_size == 0:
    algorithm_type = 'batching' + '_' + str(cpu_count) + 'cpu'
else:
    algorithm_type = 'constantbatching' + '_' + str(cpu_count) + 'cpu_batch' + str(main_batch_size)

con = psycopg2.connect(database=db_name, user="postgres", password="secret", host="localhost", port="5432")
cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)

def create_tables():
    cur.execute("drop table if exists {}".format(algorithm_type))
    cur.execute("create table {}("
                "build text,"
                "test_name text,"
                "verdict boolean,"
                "run_order int,"
                "run_time interval,"
                "main_run_order int)".format(algorithm_type))
    con.commit()
    cur.execute("drop table if exists {}_feedback".format(algorithm_type))
    cur.execute("create table {}_feedback("
                "build text,"
                "commit_time interval,"
                "start_time interval,"
                "end_time interval)".format(algorithm_type))
    con.commit()

def get_builds():
    # getting from builds table
    cur.execute("select build, min(start_time) start_time from tests_unexpected group by build, start_time order by min(run_order) asc")
    builds = cur.fetchall()
    build_count = cur.rowcount
    return builds, build_count

def get_running_builds(build_counter, builds, main_batch_size, start_time, run_time):
    if main_batch_size == 0: #batching
        batch_size = 1
        while (build_counter + batch_size) < len(builds) and \
                batch_size <= max_batch_size and \
                (builds[build_counter + batch_size]['start_time'] - start_time) <= run_time: # next build has arrived
            batch_size += 1

        if batch_size == 1: # subsequent builds are not available => only we should wait for the first one
            run_time = builds[build_counter]['start_time'] - start_time

        running_builds = builds[build_counter:build_counter + batch_size]
    else:
        batch_size = main_batch_size
        running_builds = builds[build_counter:build_counter + batch_size]
        batch_arrival = running_builds[-1]['start_time'] - start_time
        if batch_arrival > run_time: # the last build in the batch has not arrived yet, so we should wait for it and set run_time to its start_time
            run_time = batch_arrival
    build_counter += batch_size
    return build_counter, running_builds, run_time

def get_select_query():
    select_query = "select build, test_name, verdict, run_order, execution_time from tests_unexpected where build = '{}' order by run_order"
    return select_query

def get_running_tests(running_builds):
    running_tests = []
    select_query = get_select_query()
    for build in running_builds:
        filled_select_query = select_query.format(build['build'])
        cur.execute(filled_select_query)
        new_fetched_tests = cur.fetchall()
        running_tests.extend(new_fetched_tests)
    return running_tests

class test_information:
    def __init__(self, build, name, verdict, main_run_order, execution_time):
        self.build = build
        self.name = name
        self.verdict = verdict
        self.main_run_order = main_run_order
        self.execution_time = execution_time

def get_test_info(test):
    test_build = test[0]
    test_name = test[1]
    test_verdict = test[2]
    test_main_run_order = test[3]
    test_execution_time = test[4]
    test_info = test_information(test_build, test_name, test_verdict, test_main_run_order, test_execution_time)
    return test_info

def add_fail_test_build(fail_test_builds, test_info):
    if test_info.name in fail_test_builds:
        fail_test_builds[test_info.name].append(test_info)
    else:
        fail_test_builds[test_info.name] = [test_info]

def update_run_order_time(run_order, run_time, test_info, cpu_count, total_execution_time):
    run_order += 1
    try:
        run_time += test_info.execution_time / cpu_count
        total_execution_time += test_info.execution_time
    except:
        run_time += timedelta(seconds=test_info.execution_time) / cpu_count
        total_execution_time += timedelta(seconds=test_info.execution_time)
    return run_order, run_time, total_execution_time

def insert_runorder(algorithm_type, test_info, run_order, run_time):
    insert_run_order = "insert into {} (build, test_name, verdict, run_order, run_time, main_run_order)" \
                       " values(%(build)s, %(test_name)s, %(verdict)s, %(run_order)s, %(run_time)s, %(main_run_order)s)".format(
        algorithm_type)
    cur.execute(insert_run_order,
                {'build': test_info.build, 'test_name': test_info.name, 'verdict': test_info.verdict, 'run_order': run_order, 'run_time': run_time , 'main_run_order': test_info.main_run_order})
    # notice: commit outside

def insert_build_feedback(build, commit_time, start_time, end_time):
    insert_feedback = "insert into {}_feedback (build, commit_time, start_time, end_time)" \
                      " values(%(build)s, %(commit_time)s, %(start_time)s, %(end_time)s)".format(algorithm_type)
    cur.execute(insert_feedback, {'build': build, 'commit_time': commit_time, 'start_time': start_time, 'end_time': end_time})
    con.commit()

def store_builds_feedback(running_builds, run_time, builds_run_time_start, start_time):
    for b in running_builds:
        build_commit_time = b['start_time'] - start_time
        insert_build_feedback(b['build'], build_commit_time, builds_run_time_start, run_time)

def process_batch(running_builds, fail_tests, batch, run_order, run_time, cpu_count, total_execution_time):
    for build in running_builds:
        running_tests = get_running_tests([build])
        for test in running_tests:
            test_info = get_test_info(test)
            if test_info.verdict == False:
                add_fail_test_build(fail_tests, test_info)
            if test_info.name not in batch or test_info.verdict == False:
                run_order, run_time, total_execution_time = update_run_order_time(run_order, run_time, test_info, cpu_count, total_execution_time)
            # if new add to batch union
            if test_info.name not in batch:
                batch.add(test_info.name)
    return fail_tests, run_order, run_time, batch, total_execution_time


def find_store_culprit(fail_tests, running_builds, run_order, run_time, cpu_count, total_execution_time):
    for fail_test_name in fail_tests:
        test_info = fail_tests[fail_test_name][0]
        for build in running_builds:
            run_order, run_time, total_execution_time = update_run_order_time(run_order, run_time, test_info, cpu_count, total_execution_time)
            for test_info in fail_tests[fail_test_name]:
                if build['build'] == test_info.build:
                    insert_runorder(algorithm_type, test_info, run_order, run_time)
                    con.commit()
    return run_order, run_time, total_execution_time


create_tables()
builds, build_count = get_builds()
start_time = builds[0]['start_time']
run_order = 0
run_time = timedelta()
total_execution_time = timedelta()
build_counter = 0

while build_counter < len(builds):
    build_counter, running_builds, run_time = get_running_builds(build_counter, builds, main_batch_size, start_time, run_time)
    batch = set()
    fail_tests = {}
    builds_run_time_start = run_time
    fail_tests, run_order, run_time, batch, total_execution_time = process_batch(running_builds, fail_tests, batch, run_order, run_time, cpu_count, total_execution_time)
    run_order, run_time, total_execution_time = find_store_culprit(fail_tests, running_builds, run_order, run_time, cpu_count, total_execution_time)
    store_builds_feedback(running_builds, run_time, builds_run_time_start, start_time)

print("total execution time for {} algorithm with {} cpu: {} hours".format(algorithm_type, cpu_count, total_execution_time.total_seconds() / 3600))
con.close()