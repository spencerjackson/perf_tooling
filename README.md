Scripts used for QE performance evaluation.

To use:
``` sh
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt
```

To obtain Genny summary statistics as CSV:
``` sh
cd datasets/genny
./postprocess.sh genny_stats <config_yaml>
```
`config_yaml` is a YAML file containing the patch IDs, variants, and tasks for which cedar metrics are collected under the `patches` node; and the name of the tests and metrics that will be parsed from the cedar report under the `genny_metrics` node.

To obtain storage statistics as CSV:
``` sh
cd datasets/genny  # or datasets/ycsb
./postprocess.sh storage_stats <config_yaml>
```****
There must be a `storage_metrics` node in the `config_yaml` that lists the test names & metric names to parse from the cedar report.

To obtain timing statistics as CSV:
``` sh
cd datasets/genny  # or datasets/ycsb
./postprocess.sh timing_stats <config_yaml>
```
There must be a `timing_metrics` node in the `config_yaml` that lists the test names & metric names to parse from the cedar report.

To obtain YCSB summary statistics as CSV:
``` sh
cd datasets/ycsb
# first, download & extract the artifacts that contain the test output logs
./postprocess.sh fetch_artifacts <config_yaml>
# next, run update_ycsb_summary_stats to parse & gather the summary stats per execution
./postprocess.sh update_ycsb_summary_stats <config_yaml>
# lastly, run ycsb_stats to aggregate the per-execution stats into one big CSV
./postprocess.sh ycsb_stats <config_yaml>
```

To obtain Genny intra-run FTDC data (eg. for analysis in Jupyter Notebook):
``` sh
cd datasets/genny
# first, download the ftdc artifacts
./postprocess.sh fetch_ftdc <config_yaml>
# next, convert the ftdc files to json, for easier parsing in Jupyter:
./postprocess.sh ftdc_to_json <config_yaml>
```
