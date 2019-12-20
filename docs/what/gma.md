# What is Generalized Metadata Architecture (GMA)?

GMA is the name for DataHub's backend infrastructure. Unlike any existing architectures, GMA will be able to efficiently service the three most common query patterns (`document-oriented`, `complex` & `graph` queries, and `fulltext search`) 
— with minimal onboarding efforts needed. 

GMA also embraces a distributed model, where each team owns, develops and operates their own [metadata services](gms.md), while the metadata are automatically aggregated to populate the central metadata graph and search indexes. 
This is made possible by standardizing the metadata models and the access layer. 
We strongly believe that GMA can bring tremendous leverage to any team that has a need to store and access metadata. 
Moreover, standardizing metadata modeling promotes a model-first approach to developments, resulting in a more concise, consistent, and highly connected metadata ecosystem that benefits all DataHub users.