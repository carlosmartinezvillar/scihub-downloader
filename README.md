# Sentinel API Retriever

## How-to

```
python3 download,py -g <geo_file>
```

will iterate through the list of coordinates in <geo_file> and find the products intersecting the coordinates in each line.

```
python3 download.py -g ./dat/sites_small.txt
```

```
python3 download.py -f <DATA_DIR>/offline.tsv
```

```
kubectl create -f first_download_job.yml
```

```
kubectl create -f offline_update_job.yml
```

## Files and directory

The directory is organized as follows:

```
.
├── README.md
├── cfg
│	├── first_download_template.yml
│	└── offline_update_template.yml
├── dat
│	├── sites.txt
│	├── sites_small.txt
│	├── sites_small_table.csv
│	└── sites_table.csv
├── download.py
└── yaml_template.yml
```

```
1 */1 * * * /usr/local/bin/kubectl delete -f /Users/cimv/Desktop/sentinel-api-retriever/cfg/img-processor_job_1.yml >> /Users/cimv/Desktop/sentinel-api-retriever/log/cron.log

5 */1 * * * /usr/local/bin/kubectl create -f /Users/cimv/Desktop/sentinel-api-retriever/cfg/img-processor_job_1.yml >> /Users/cimv/Desktop/sentinel-api-retriever/log/cron.log
```