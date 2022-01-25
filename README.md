# fuse-analysis-immcellfie
Dependent on the following containers which will be pulled in the docker-compose file:
* tx-persistent
* redis

The following containers support the endpoints that are called from this repo:

* txscience/fuse-mapper-immunespace:0.1
* txscience/tx-immunespace-groups:0.3
* hmasson/cellfie-standalone-app:v2

For example, `docker image ls` should list the above 3 containers
