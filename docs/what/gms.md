# What is Generalized Metadata Service (GMS)?



## Central vs Distributed
Any entity such as `datasets` or `users` which are [onboarded](../how/entity-onboarding.md) to GMA can be designed to be served through a separate microservice.
These microservices are called GMS. Although we've a central GMS ([datahub-gms](../../gms)) which serves all entities, 
we could easily have multiple GMS, each has responsible for a specific entity such as `dataset-gms`, `user-gms` etc. 