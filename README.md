# ContrastTestOptimizations Replication Package

## Getting Started
The following instructions help you to get a copy of project up and running.

### Prerequisites
To run this project you need to install the following:
* Python 3.7 or higher
* PostgreSQL 10.18 or higher

### Installation
1. The data is from the paper:
Fallahzadeh, Emad, and Peter C. Rigby. "The Impact of Flaky Tests on Historical Test Prioritization on Chrome." 2022 IEEE/ACM 44th International Conference on Software Engineering: Software Engineering in Practice (ICSE-SEIP). IEEE, 2022.

Download the data from https://doi.org/10.5281/zenodo.5576626
2. Unzip compressed files by the following command in terminal:
> cat x*.gz.part | tar -x -vz -f -
3. Execute following command to create the chromium database in terminal:
> createdb chromium
4. To import test table run the following:
> psql -U username -d database -1 -f chromium_dump.sql

### Usage
1. Run the following commands to prepare tables:
> psql chromium -f convert_chromium_unexpected.sql

2. In the following scripts replace ‘secret’ in the psycopg2.connect() with database password you set.
3. To remove repeated tests in each build run:
> python3 RemoveRepeatedTestsInEachBuild.py -t tests_unexpected
4. set cpu_count to the desired number of machines in config.conf
5. Run the following commands to get the results from the algorithms:

TestAll:
> python3 TestAll.py

KimPorter:
> python3 KimPorterTestPrioritization.py

ElbaumPrioritization:
> python3 ElbaumTestPrioritization.py

ElbaumSelection:
> python3 ElbaumTestSelection.py

Batching:

set batch_size = 0 in config.conf
> python3 Batching.py


6. To analyze the results:
- To get median feedback time:
> python3 AnalyzeResults.py get_median_feedback_time

- To get confidence intervals for all different combinations of algorithms and number of machines:
> python3 AnalyzeResults.py get_all_feedback_confidence_intervals

- To get p_value and effect sizes between the feedback time distribution of different algorithms and number of machines:
> python3 AnalyzeResults.py get_feedback_distribution_differences

- To get GainedTime for different algorithms:
> python3 AnalyzeResults.py get_gained_time

- To get p_value and effect sizes between the GainedTime distribution of different algorithms and number of machines:
> python3 AnalyzeResults.py get_gained_time_distribution_differences
