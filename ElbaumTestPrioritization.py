import psycopg2
import psycopg2.extras
import configparser
import math
from datetime import datetime, timedelta
import time

config_parser = configparser.RawConfigParser()
config_file_path = r'config.conf'
config_parser.read(config_file_path)
main_batch_size = 0
db_name = config_parser.get('General', 'db_name')
max_batch_size = 2 # it works as prioritization_window
cpu_count = int(config_parser.get('General', 'cpu_count'))
failure_window_size = 0 # 0=unlimited
execution_window_size = 2

reprioritize = False
use_time_window = False



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
    if main_batch_size == 0: # dynamic batching
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
    return build_counter, running_builds, run_time, batch_size


def get_select_query():
    select_query = "select build, test_name, verdict, run_order, execution_time from tests_unexpected where build = '{}' order by run_order"
    return select_query


def get_new_tests(running_builds, builds_test_count):
    running_tests = []
    select_query = get_select_query()
    for build in running_builds:
        build_id = build['build']
        filled_select_query = select_query.format(build_id)
        cur.execute(filled_select_query)
        new_fetched_tests = cur.fetchall()
        running_tests.extend(new_fetched_tests)
        builds_test_count[build_id] = len(new_fetched_tests)
    return running_tests, builds_test_count

def test_in_failure_window(failure_window, test_name):
    if test_name in failure_window.tempset:
        return True
    for f in failure_window.list:
        if test_name in f:
            return True

def test_in_execution_window(execution_window, test_name):
    if test_name in execution_window.tempset:
        return True
    for e in execution_window.list:
        if test_name in e:
            return True

def calculate_score(failure_window, execution_window, executed_tests, test_name):
    score = 0
    # time since last failure <= w_f or time since last execution > w_e or test is new
    if(test_in_failure_window(failure_window, test_name) or not test_in_execution_window(execution_window, test_name) or test_name not in executed_tests ):
         score = 1
    return score


def rescore_prioritized_tests(prioritized_tests, failure_window, execution_window, executed_tests):
    for test in prioritized_tests:
        score = calculate_score(failure_window, execution_window, executed_tests, test[1])
        test[5] = score
    return prioritized_tests


def append_prioritized_tests(tests_to_append, prioritized_tests, failure_window, execution_window, executed_tests):
    for test in tests_to_append:
        score = calculate_score(failure_window, execution_window, executed_tests, test['test_name'])
        scoredItem = list(test)
        scoredItem.append(score)
        prioritized_tests.append(scoredItem)
    return prioritized_tests


def prioritize_tests(prioritized_tests, new_fetched_tests, failure_window, execution_window, executed_tests):
    prioritized_tests = rescore_prioritized_tests(prioritized_tests, failure_window, execution_window, executed_tests)
    prioritized_tests = append_prioritized_tests(new_fetched_tests, prioritized_tests, failure_window, execution_window, executed_tests)
    prioritized_tests.sort(key=lambda x: x[5], reverse=True)  # prioritize based on score


def reprioritize_tests(prioritized_tests, failure_window, failure_window_temp, execution_window, execution_window_temp, executed_tests):
    failure_window.tempset = failure_window_temp
    execution_window.tempset = execution_window_temp
    prioritized_tests = rescore_prioritized_tests(prioritized_tests, failure_window, execution_window, executed_tests)
    prioritized_tests.sort(key=lambda x: x[5], reverse=True)  # prioritize based on score


def initialize_counters_sets():
    failure_window_temp = set()
    execution_window_temp = set()
    return failure_window_temp, execution_window_temp


class test_information:
    def __init__(self, build, name, verdict, main_run_order, execution_time, score):
        self.build = build
        self.name = name
        self.verdict = verdict
        self.main_run_order = main_run_order
        self.execution_time = execution_time
        self.score = score


def get_first_test(tests):
    test = tests.pop(0)
    test_build = test[0]
    test_name = test[1]
    test_verdict = test[2]
    test_main_run_order = test[3]
    test_execution_time = test[4]
    test_score = test[5]
    test_info = test_information(test_build, test_name, test_verdict, test_main_run_order, test_execution_time, test_score)
    return test_info


def update_run_order_time(run_order, run_time, test_info, cpu_count, total_execution_time, execution_window_temp, executed_tests, builds_processed_test_count):
    run_order += 1
    try:
        run_time += test_info.execution_time / cpu_count
        total_execution_time += test_info.execution_time
    except:
        run_time += timedelta(seconds=test_info.execution_time) / cpu_count
        total_execution_time += timedelta(seconds=test_info.execution_time)
    execution_window_temp.add(test_info.name)
    executed_tests.add(test_info.name)
    builds_processed_test_count[test_info.build] = builds_processed_test_count[test_info.build] + 1 if test_info.build in builds_processed_test_count else 1
    return run_order, run_time, total_execution_time, execution_window_temp, executed_tests, builds_processed_test_count


def update_failures(test_name, failure_window_temp):
    failure_window_temp.add(test_name)


def insert_runorder(algorithm_type, test_info, run_order, run_time):
    insert_run_order = "insert into {} (build, test_name, verdict, run_order, run_time, main_run_order)" \
                       " values(%(build)s, %(test_name)s, %(verdict)s, %(run_order)s, %(run_time)s, %(main_run_order)s)".format(
        algorithm_type)
    cur.execute(insert_run_order,
                {'build': test_info.build, 'test_name': test_info.name, 'verdict': test_info.verdict, 'run_order': run_order, 'run_time': run_time , 'main_run_order': test_info.main_run_order})
    # notice: commit outside


def update_sets(failure_window, failure_window_temp, execution_window, execution_window_temp):
    failure_window.add(failure_window_temp)
    execution_window.add(execution_window_temp)
    return failure_window, execution_window

class window:
    def __init__(self, size):
        self.index = 0
        self.size = size
        self.list = []
        self.tempset = set()
        for i in range(0, size):
            self.list.append(set())

    def add(self, window_temp):
        if(self.size == 0):
            self.tempset = self.tempset | window_temp # union of the two sets
        else:
            self.tempset = set()
            self.list[self.index] = window_temp # fill one of the window sets based on the size of the window and its index
            self.index = (self.index + 1) % self.size

def insert_build_feedback(build, commit_time, start_time, end_time):
    insert_feedback = "insert into {}_feedback (build, commit_time, start_time, end_time)" \
                      " values(%(build)s, %(commit_time)s, %(start_time)s, %(end_time)s)".format(algorithm_type)
    cur.execute(insert_feedback, {'build': build, 'commit_time': commit_time, 'start_time': start_time, 'end_time': end_time})
    con.commit()

def store_builds_feedback(running_builds, run_time, builds_run_time_start, start_time):
    for b in running_builds:
        build_commit_time = b['start_time'] - start_time
        insert_build_feedback(b['build'], build_commit_time, builds_run_time_start, run_time)

def find_running_build(running_builds, build_id):
    for b in running_builds:
        if b['build'] == build_id:
            return b


if __name__ == '__main__':
    algorithm_type = 'elbaum_prioritization_' + str(cpu_count) + 'cpu_' + str(execution_window_size) + 'we_' \
                     + str(failure_window_size) + 'wf_' + str(max_batch_size) + 'wp' #prioritization window
    con = psycopg2.connect(database=db_name, user="postgres", password="secret", host="localhost", port="5432")
    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)

    start = time.time()
    prioritized_tests = []

    failure_window = window(failure_window_size)
    execution_window = window(execution_window_size)
    executed_tests = set()
    run_order = 0
    run_time = timedelta()
    total_execution_time = timedelta()

    create_tables()
    builds, build_count = get_builds()
    start_time = builds[0]['start_time']
    build_counter = 0
    builds_test_count = {}
    builds_processed_test_count = {}

    while build_counter < len(builds):
        build_counter, running_builds, run_time, batch_size = get_running_builds(build_counter, builds, main_batch_size, start_time, run_time)
        builds_run_time_start = run_time
        new_fetched_tests, builds_test_count = get_new_tests(running_builds, builds_test_count)
        # rescore remaining tests from previous runs + add new ones
        prioritize_tests(prioritized_tests, new_fetched_tests, failure_window, execution_window, executed_tests)

        failure_window_temp, execution_window_temp = initialize_counters_sets()
        for i in range(0, len(prioritized_tests)): #test in prioritized_tests:
            test_info = get_first_test(prioritized_tests)
            run_order, run_time, total_execution_time, execution_window_temp, executed_tests, builds_processed_test_count = update_run_order_time(run_order, run_time, test_info, cpu_count, total_execution_time, execution_window_temp, executed_tests, builds_processed_test_count)
            if test_info.verdict == False:
                update_failures(test_info.name, failure_window_temp)
                if (reprioritize == True):
                    reprioritize_tests(prioritized_tests, failure_window, failure_window_temp, execution_window, execution_window_temp, executed_tests)
                insert_runorder(algorithm_type, test_info, run_order, run_time)
                con.commit()
            if builds_processed_test_count[test_info.build] >= builds_test_count[test_info.build]: # all build's tests are processed
                running_build = find_running_build(running_builds, test_info.build)
                store_builds_feedback([running_build], run_time, builds_run_time_start, start_time)

        failure_window, execution_window = update_sets(failure_window, failure_window_temp, execution_window, execution_window_temp)

    print("total execution time for {} algorithm with {} cpu: {} hours".format(algorithm_type, cpu_count, total_execution_time.total_seconds() / 3600))
    con.close()
    end = time.time()
    print(end - start)