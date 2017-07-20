# WhereHows Roadmap

Short term:
* Improve search algorithm
* Convert most of the ETL jobs to using [Gobblin](https://github.com/linkedin/gobblin)
* Allow data platforms to push metadata changes to WhereHows directly via [Kafka](https://kafka.apache.org/) events

Medium term:
* Store lineage metadata in graph DBs and allow querying
* Generalize storage backend to support NoSQL, [Ground](https://github.com/ground-context/ground) etc.
* Add an admin interface

Long Term:
* Support column-level lineage
* Fully support other types of entities, such as metrics, dashboards, experiments etc. as well as general relationships. 
