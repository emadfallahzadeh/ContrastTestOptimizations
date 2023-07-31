import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import configparser
import psycopg2.extras
import seaborn as sns
import datetime
import sys

config_parser = configparser.RawConfigParser()
config_file_path = r'config.conf'
config_parser.read(config_file_path)
batch_size = int(config_parser.get('General', 'batch_size'))
db_name = config_parser.get('General', 'db_name')
cpu_count = int(config_parser.get('General', 'cpu_count')) # number of machines

con = psycopg2.connect(database=db_name, user="postgres", password="secret", host="localhost", port="5432")
cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)

def get_unit_divider(unit):
    if unit == 'second':
        unit_divider = 1
    elif unit == 'minute':
        unit_divider = 60
    if unit == 'hour':
        unit_divider = 3600
    elif unit == 'day':
        unit_divider = 86400
    return unit_divider
#################
def get_median_feedback_time(unit='hour'):
    unit_divider = get_unit_divider(unit)
    algorthms_types = ['elbaum_selection', 'testall', 'kimporter', 'elbaum_prioritization', 'elbaum_selection', 'batchall']
    w_e=2
    w_f=0
    windows_str=str(w_e)+ 'we_' + str(w_f) + 'wf'
    for algorithm in algorthms_types:
        if algorithm == 'elbaum_selection':
            algorithm_feedback = algorithm + '_' + str(cpu_count) + 'cpu_'+ windows_str +'_feedback'
        else:
            algorithm_feedback = algorithm + '_' + str(cpu_count) + 'cpu_feedback'
        query = 'select PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY a.end_time - a.commit_time) ' \
                'from {} a'.format(algorithm_feedback)
        cur.execute(query)
        median_feedback_time = cur.fetchone()[0]
        print("median feedback time for " + algorithm_feedback + ": " + str(median_feedback_time.total_seconds()/unit_divider) + " in hours")

###################
def Is_normally_distributed(algorithm_feedback):
    if not algorithm_feedback:
        algorithm = 'batchall'
        algorithm_feedback = f"{algorithm}_{cpu_count}cpu_feedback"
    query = f"SELECT (end_time - commit_time) AS feedback FROM {algorithm_feedback}"
    cur.execute(query)
    df = pd.DataFrame(cur.fetchall(), columns=['feedback'])
    df['feedback'] = df['feedback'].dt.total_seconds()
    feedback_times = df['feedback'].tolist()

    # Shapiro-Wilk test for normality
    statistic, p_value = stats.shapiro(feedback_times)
    alpha = 0.05
    return p_value > alpha

def calculate_confidence_interval_normal(data, statistic, confidence=0.95):
    n = len(data)
    stat = statistic(data)
    #print(f"Mean: {mean}")
    std_dev = np.std(data, ddof=1)  # Unbiased estimator for sample standard deviation
    margin_error = stats.norm.ppf((1 + confidence) / 2) * std_dev / np.sqrt(n)
    lower_bound = (stat - margin_error)
    upper_bound = (stat + margin_error)
    return lower_bound, upper_bound

# Function to calculate the desired statistic (e.g., mean, median, etc.)
def calculate_mean(data):
    return np.mean(data)

def calculate_median(data):
    return np.median(data)

# Function to generate bootstrap samples
def generate_bootstrap_sample(data):
    n = len(data)
    bootstrap_sample = np.random.choice(data, size=n, replace=True)
    return bootstrap_sample

# Function to calculate the bootstrap distribution
def calculate_bootstrap_distribution(data, statistic, n_iterations):
    bootstrap_distribution = []
    for _ in range(n_iterations):
        bootstrap_sample = generate_bootstrap_sample(data)
        stat = statistic(bootstrap_sample)  # Call the provided statistic function
        bootstrap_distribution.append(stat)
    return bootstrap_distribution

# Function to calculate the confidence interval without assuming normal distribution
def calculate_confidence_interval_not_normal(data, statistic, confidence_level=0.95, n_iterations=1000):
    bootstrap_distribution = calculate_bootstrap_distribution(data, statistic, n_iterations)
    lower_percentile = (1 - confidence_level) / 2
    upper_percentile = 1 - lower_percentile
    lower_bound = np.percentile(bootstrap_distribution, lower_percentile * 100)
    upper_bound = np.percentile(bootstrap_distribution, upper_percentile * 100)
    return lower_bound, upper_bound

def calculate_feedback_confidence_interval(algorithm_feedback, unit, statistic):
    if not algorithm_feedback:
        algorithm = 'batchall'
        algorithm_feedback = f"{algorithm}_{cpu_count}cpu_feedback"
    query = f"SELECT (end_time - commit_time) AS feedback FROM {algorithm_feedback}"
    cur.execute(query)
    df = pd.DataFrame(cur.fetchall(), columns=['feedback'])
    df['feedback'] = df['feedback'].dt.total_seconds()
    feedback_times = df['feedback'].tolist()

    if Is_normally_distributed(algorithm_feedback):
        confidence_interval = calculate_confidence_interval_normal(feedback_times, statistic)
    else:
        confidence_interval = calculate_confidence_interval_not_normal(feedback_times, statistic)
    unit_divider = get_unit_divider(unit)
    lower_bound_scaled = confidence_interval[0] / unit_divider
    upper_bound_scaled = confidence_interval[1] / unit_divider
    print(f"Confidence Interval {algorithm_feedback}: {lower_bound_scaled}, {upper_bound_scaled}")
    return lower_bound_scaled, upper_bound_scaled

def get_all_feedback_confidence_intervals(unit='minute', statistic = calculate_median):
    windows = {'w_e': 2, 'w_f': 0, 'w_p': 2}
    algorithms_types = ['testall', 'kimporter', 'elbaum_prioritization', 'elbaum_selection', 'batchall']
    cpu_counts = [8, 16] + list(range(25, 401, 25))
    column_names = ['cpu count']
    for algorithm in algorithms_types:
        column_names.append(f"{algorithm} lower bound")
        column_names.append(f"{algorithm} upper bound")

    results = []
    for cpu_count in cpu_counts:
        row = [cpu_count]
        for algorithm in algorithms_types:
            algorithm_feedback = generate_algorithm_feedback(algorithm, cpu_count, windows)
            confidence_interval = calculate_feedback_confidence_interval(algorithm_feedback, unit, statistic)
            row.extend(confidence_interval)
        results.append(row)

    df = pd.DataFrame(results, columns=column_names)
    df.to_excel('Results/confidence_intervals.xlsx', index=False)

##################
def generate_algorithm_feedback(algorithm, cpu_count, windows):
    if algorithm.startswith('elbaum_selection'):
        algorithm_feedback = f"{algorithm}_{cpu_count}cpu_{windows['w_e']}we_{windows['w_f']}wf_feedback"
    elif algorithm.startswith('elbaum_prioritization'):
        algorithm_feedback = f"{algorithm}_{cpu_count}cpu_{windows['w_e']}we_{windows['w_f']}wf_{windows['w_p']}wp_feedback"
    else:
        algorithm_feedback = f"{algorithm}_{cpu_count}cpu_feedback"

    return algorithm_feedback
def get_feedback_data(algorithm_feedback):
    query = f"SELECT (end_time - commit_time) AS feedback FROM {algorithm_feedback}"
    cur.execute(query)
    df = pd.DataFrame(cur.fetchall(), columns=['feedback'])
    df['feedback'] = df['feedback'].dt.total_seconds()
    return df['feedback'].tolist()

def perform_comparison(cpu_data, algorithm1, algorithm2):
    # Pairwise Mann-Whitney U test
    u, p_value = stats.mannwhitneyu(cpu_data[algorithm1], cpu_data[algorithm2])
    # Cliff's delta effect size
    effect_size = (2 * u - len(cpu_data[algorithm1]) * len(cpu_data[algorithm2])) / (len(cpu_data[algorithm1]) * len(cpu_data[algorithm2]))
    return p_value, effect_size

def get_feedback_distribution_differences():
    windows = {'w_e': 2, 'w_f': 0, 'w_p': 2}
    algorithms_types = ['testall', 'kimporter', 'elbaum_prioritization', 'elbaum_selection', 'batchall']
    cpu_counts = [8, 16] + list(range(25, 401, 25))
    results = []
    cpu_data = {}
    for cpu_count in cpu_counts:
        for algorithm in algorithms_types:
            algorithm_feedback = generate_algorithm_feedback(algorithm, cpu_count, windows)
            cpu_data[algorithm] = get_feedback_data(algorithm_feedback)

        for algorithm1, algorithm2 in combinations(algorithms_types, 2):
            p_value, effect_size = perform_comparison(cpu_data, algorithm1, algorithm2)
            results.append({
                'CPUs': cpu_count,
                'Comparison': algorithm1 + ' vs ' + algorithm2,
                'p-value': p_value,
                'Effect Size': effect_size
            })

    df_results = pd.DataFrame(results)
    df_pivot = df_results.pivot(index='CPUs', columns='Comparison', values=['p-value', 'Effect Size'])
    df_pivot.to_excel('Results/pairwise_comparison_results.xlsx', index=True, float_format='%.20f')

###################
def get_gained_time(unit='hour'):
    unit_divider = get_unit_divider(unit)
    w_e = 2
    w_f = 0
    w_p = 2
    # be cautious about cpu_count
    query = "select f.build, f.test_name, f.run_time f_run_time," \
            " k.run_time k_run_time, (f.run_time - k.run_time) k_run_time_diff," + \
            " e.run_time e_run_time, (f.run_time - e.run_time) e_run_time_diff," + \
            " es.run_time es_run_time, (f.run_time - es.run_time) es_run_time_diff," + \
            " d.run_time d_run_time, (f.run_time - d.run_time) d_run_time_diff" + \
            " from testall_{0}cpu as f, kimporter_{0}cpu as k, elbaum_{0}cpu_{1}we_{2}wf_{3}wp as e, elbaum_selection_{0}cpu_{1}we_{2}wf as es, batchall_{0}cpu as d".format(cpu_count, w_e, w_f, w_p) + \
            " where f.verdict = false and f.main_run_order = k.main_run_order and f.main_run_order = e.main_run_order and f.main_run_order = es.main_run_order and f.main_run_order = d.main_run_order"

    df = pd.read_sql_query(query, con)

    df.to_csv('Results/'+ db_name + '_gains.csv', sep=',')
    dc = df.describe()
    print('description:')
    print(round(dc,2))

    print('testall median failure run time: ' + str(df['f_run_time'].median().total_seconds()/unit_divider) + ' in ' + unit)
    print('kimporter median failure run time: ' + str(df['k_run_time'].median().total_seconds()/unit_divider) + ' in ' + unit)
    print('kimporter median failure gained time: ' + str(df['k_run_time_diff'].median().total_seconds()/unit_divider) + ' in ' + unit)
    print('elbaum_prioritization median failure run time: ' + str(df['e_run_time'].median().total_seconds()/unit_divider) + ' in ' + unit)
    print('elbaum_prioritization median failure gained time: ' + str(df['e_run_time_diff'].median().total_seconds()/unit_divider) + ' in ' + unit)
    print('elbaum_selection median failure run time: ' + str(df['es_run_time'].median().total_seconds()/unit_divider) + ' in ' + unit)
    print('elbaum_selection median failure gained time: ' + str(df['es_run_time_diff'].median().total_seconds()/unit_divider) + ' in ' + unit)
    print('batchall median failure run time: ' + str(df['d_run_time'].median().total_seconds()/unit_divider) + ' in ' + unit)
    print('batchall median failure gained time: ' + str(df['d_run_time_diff'].median().total_seconds()/unit_divider) + ' in ' + unit)


###################
def perform_comparison(cpu_data, algorithm1, algorithm2):
    # Pairwise Mann-Whitney U test
    u, p_value = stats.mannwhitneyu(cpu_data[algorithm1], cpu_data[algorithm2])
    # Cliff's delta effect size
    effect_size = (2 * u - len(cpu_data[algorithm1]) * len(cpu_data[algorithm2])) / (len(cpu_data[algorithm1]) * len(cpu_data[algorithm2]))
    return p_value, effect_size

def get_gained_times(cpu_count, windows):
    query = "select (f.run_time - k.run_time) kimporter," + \
            " (f.run_time - e.run_time) elbaum_prioritization," + \
            " (f.run_time - es.run_time) elbaum_selection," + \
            " (f.run_time - d.run_time) batchall" + \
            f" from testall_{cpu_count}cpu as f, kimporter_{cpu_count}cpu as k, elbaum_{cpu_count}cpu_{windows['w_e']}we_{windows['w_f']}wf_{windows['w_p']}wp as e," \
            f" elbaum_selection_{cpu_count}cpu_{windows['w_e']}we_{windows['w_f']}wf as es, batchall_{cpu_count}cpu as d" + \
            " where f.verdict = false and f.main_run_order = k.main_run_order and f.main_run_order = e.main_run_order and f.main_run_order = es.main_run_order and f.main_run_order = d.main_run_order"
    df = pd.read_sql_query(query, con)
    return df

def get_gained_time_distribution_differences():
    windows = {'w_e': 2, 'w_f': 0, 'w_p': 2}
    algorithms_types = ['kimporter', 'elbaum_prioritization', 'elbaum_selection', 'batchall']
    cpu_counts = [8, 16] + list(range(25, 401, 25))
    results = []
    cpu_data = {}
    for cpu_count in cpu_counts:
        gained_times_df = get_gained_times(cpu_count, windows)
        for algorithm in algorithms_types:
            cpu_data[algorithm] = gained_times_df[algorithm].dt.total_seconds().tolist()

        for algorithm1, algorithm2 in combinations(algorithms_types, 2):
            p_value, effect_size = perform_comparison(cpu_data, algorithm1, algorithm2)
            results.append({
                'CPUs': cpu_count,
                'Comparison': algorithm1 + ' vs ' + algorithm2,
                'p-value': p_value,
                'Effect Size': effect_size
            })

    df_results = pd.DataFrame(results)
    df_pivot = df_results.pivot(index='CPUs', columns='Comparison', values=['p-value', 'Effect Size'])
    df_pivot.to_excel('Results/pairwise_gained_time_comparison_results.xlsx', index=True, float_format='%.20f')


get_median_feedback_time() # cpu_count in config
get_all_feedback_confidence_intervals() # cpu_count in config
get_feedback_distribution_differences() # median feedback time p_value, effect_size
get_gained_time() # cpu_count in config
get_gained_time_distribution_differences() # gained_time p_value, effect_size


def main():
    methods = {
        'get_median_feedback_time': get_median_feedback_time,
        'get_all_feedback_confidence_intervals': get_all_feedback_confidence_intervals,
        'get_feedback_distribution_differences': get_feedback_distribution_differences,
        'get_gained_time': get_gained_time,
        'get_gained_time_distribution_differences': get_gained_time_distribution_differences,
    }

    if len(sys.argv) < 2:
        print("Please provide a method name as a command-line argument.")
        return

    method_name = sys.argv[1]
    selected_method = methods.get(method_name)
    if selected_method:
        selected_method()
    else:
        print("Invalid method name. Available methods:")
        print(", ".join(methods.keys()))

if __name__ == "__main__":
    main()

