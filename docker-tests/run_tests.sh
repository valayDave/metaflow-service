python fixed-db-tests.py --database-password password \
                        --datastore s3 \
                        --results-output-path all-versions-on-ui.json \
                        --dont-remove-containers

python deploy-ui.py --database-password password