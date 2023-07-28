import psycopg2
import psycopg2.extras
import configparser
from datetime import datetime, timedelta

config_parser = configparser.RawConfigParser()
config_file_path = r'config.conf'
config_parser.read(config_file_path)
db_name = config_parser.get('General', 'db_name')
cpu_count = int(config_parser.get('General', 'cpu_count'))
algorithm_type = 'testall_' + str(cpu_count) + 'cpu'

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

def update_failures(test_name):
    # insert or update failures table
    update_query = "insert into failures values(%(testname)s, 1) " \
                   "on conflict (test_name) do " \
                   "update set fails = failures.fails + 1"
    cur.execute(update_query, {'testname': test_name})


def insert_runorder(algorithm_type, test, run_order, run_time):
    insert_run_order = "insert into {} (build, test_name, verdict, run_order, run_time, main_run_order)" \
                       " values(%(build)s, %(test_name)s, %(verdict)s, %(run_order)s, %(run_time)s, %(main_run_order)s)".format(
        algorithm_type)
    cur.execute(insert_run_order,
                {'build': test['build'], 'test_name': test['test_name'], 'verdict': test['verdict'],
                 'run_order': run_order, 'run_time': run_time, 'main_run_order': test['run_order']})
    # notice: commit outside

def process_builds(running_builds, cpu_count, run_order, run_time, total_execution_time):
    running_tests = get_running_tests(running_builds)
    for test in running_tests:
        run_order += 1
        run_time += test['execution_time'] / cpu_count
        total_execution_time += test['execution_time']
        if test['verdict'] == False:
            #update_failures(test['test_name'])
            insert_runorder(algorithm_type, test, run_order, run_time)
    con.commit()
    return run_order, run_time, total_execution_time

def insert_build_feedback(build, commit_time, start_time, end_time):
    insert_feedback = "insert into {}_feedback (build, commit_time, start_time, end_time)" \
                      " values(%(build)s, %(commit_time)s, %(start_time)s, %(end_time)s)".format(algorithm_type)
    cur.execute(insert_feedback, {'build': build, 'commit_time': commit_time, 'start_time': start_time, 'end_time': end_time})
    con.commit()

def store_builds_feedback(running_builds, run_time, builds_run_time_start, start_time):
    for b in running_builds:
        build_commit_time = b['start_time'] - start_time
        insert_build_feedback(b['build'], build_commit_time, builds_run_time_start, run_time)

create_tables()
builds, build_count = get_builds()
start_time = builds[0]['start_time']
run_order = 0
run_time = timedelta()
total_execution_time = timedelta()

for l in range(0, build_count):
    running_build = builds[l]
    build_arrival = running_build['start_time'] - start_time
    if build_arrival > run_time:
        run_time = build_arrival
    builds_run_time_start = run_time
    run_order, run_time, total_execution_time = process_builds([running_build], cpu_count, run_order, run_time, total_execution_time)
    store_builds_feedback([running_build], run_time, builds_run_time_start, start_time)

print("total execution time for {} algorithm with {} cpu: {} hours".format(algorithm_type, cpu_count, total_execution_time.total_seconds() / 3600))
con.close()
