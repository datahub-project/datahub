# RBAC: Fine-grained Access Controls in DataHub 

## Abstract

Access control is about managing what operations can be performed by whom. There are 2 broad buckets comprising access control:

- **Authentication**: Logging in. Associating an actor with a known identity.
- **Authorization**: Performing an action. Allowing / denying known identities to perform specific types of operations.

Over the past few months, numerous requests have surfaced around controlling access to metadata stored inside DataHub. 
In this doc, we will propose a design for supporting pluggable authentication along with fine-grained authorization within DataHub's backend (GMS).

## Requirements

We will cover the use cases around access control in this section, gathered from a multitude of sources.

### Personas

This feature is targeted primarily at the DataHub **Operator** & **Admin** personas (often the same person). This feature can help admins of DataHub comply with their respective company policies.

The secondary beneficiary are **Data Users** themselves. Fine-grained access controls will permit Data Owners, Data Stewards to more tightly control the evolution of the metadata under management. It will also make it more difficult to make mistakes while changing metadata, such as accidentally overriding / removing good metadata authored by someone else.

### Community Asks

Sheetal Pratik (Saxo Bank)

**Asks**

- Model metadata "domains" (ie. resource scopes, namespaces) using DataHub
- Define roles that are scoped to a particular domain 
- Ability to define READ / UPDATE / DELETE policies against DataHub resources at the following granularities: 
    - individual resource (primary key based)
    - resource type (eg. all 'datasets')
    - resource domain 
    which can be associated with requests against DataHub backend via mapping from resolved Actor information (principal / username, groups, etc). 
    Resources can include entities, their aspects, roles, policies, etc. 
- Ability to compose & reuse groups of access policies.  
- Support for integrating with Active Directory (users, groups, and mappings to access policies)

Alasdair McBride (G-Research)

**Asks**

- Ability to organize multiple assets into groups and assign bucketed policies to these groups. 
- Ability to define READ / UPDATE / DELETE policies against DataHub resources at the following granularities:
    - individual resource (primary key based)
    - resource type (eg. all 'datasets')
    - resource group 
      which can be associated with requests against DataHub backend via mapping from resolved Actor information (principal / username, groups, etc).
      Resources can include entities, their aspects, roles, policies, etc.
- Leverage DataHub backend APIs to discover metadata assets programmatically using service principals. 
- Support for integrating with Active Directory (users, groups, and mappings to access policies)


As you may have noticed, the concepts of "domain" and "group" described in each set of requirements are quite similar. From here
on out, we will refer to a bucket of related entities that should be managed together as a metadata "domain".

### Consolidated User Stories

|As a...          |I want to..                                                                                         |Because..                                                                                                                                       |
|-----------------|----------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------|
|DataHub Operator |Restrict the types of metadata that certain teams / individuals can change.                         |Reduce the changes of mistakes or malicious changes to metadata. Improve quality of metadata by putting it in the hands of the most knowledgable|
|DataHub Operator |Restrict the types of metadata that certain teams / individuals can view.                           |Reduce the risk of falling out of compliance by displaying sensitive data in the Metadata UI (sample data values & beyond)                      |
|DataHub Operator |Grant the ability to manage access policies to other users of DataHub.                              |I want to delegate this task to individual team managers. (Large org)                                                                           |
|DataHub Operator |Define bounded contexts, or "domains", of related metadata that can be access controlled together   |I want to empower teams with most domain knowledge to manage their own access controls.                                                         |
|DataHub Operator |Map users & groups from 3rd party identity providers to resolved access policies                    |I want to reuse the identity definitions that my organization already has                                                                       |
|DataHub Operator |Create identities for services and associate them with policies. (service principals)               |I want to access DataHub programmatically while honoring with restricted access controls.                                                       |
|DataHub User     |Update Metadata that I know intimately. For example, table descriptions.                            |I want to provide high-quality metadata to my consumers.                                                                                        |


### Concrete Requirements


#### Must Haves

a. a central notion of Actor identity in the DataHub backend (GMS).

b. pluggable authentication responsible for resolving DataHub Actors 
    
    - in scope: file-based username password plugin (for built-in roles), continue to support OIDC 
    - in the future: saml, ldap / ad, api key, native authentication plugins

c. ability to define logical access control policies based on a combination of

    - resource type: the type of resource being accessed on the DataHub platform (eg. dataset entity, dataset aspect, roles, privileges etc) (exact match or ALL)
    - resource domain: a collection of related resources grouped below the default "global" level (scope / namespace) that is associated with a resource (exact match)
    - resource identifier: the primary key identifier for a resource (eg. dataset urn) (support for pattern matching)
    - action (bound to resource type. eg. read / write) 

with support for optional conjunctions of filtering on resource type, domain, & identifier (eg. resource identifier = "urn:li:dataset:1" AND domain = "sales") 
and including with support for the following resource types:

    - metadata entities: datasets, charts, dashboards, etc. 
    - metadata aspects: dataset ownership, chart info, etc. 
    - access control objects: roles, policies, etc.

d. ability to define named roles that can be associated with fine-grained access control policies

e. ability to configure mapping rules from DataHub Actors to named roles
    
    - where Actor = (principal name, group names, freeform string properties) 

g. ability to manage roles, role mappings, policies dynamically via Rest API

h. ability to enforce fine-grained access control policies (ref.b) (Authorizer implementation)
    - Inputs: resolved roles, role-policy mappings, resource type, resource domain, resource key

## Nice to Haves

a. policies that are tied to arbitrary attributes of a target resource object.

b. ability to manage roles, role mappings, policies via React UI

## What success looks like

Based on the requirements gathered from talking with folks in the community, we decided to rally around the following goal. It should be possible to 

1. Define a new access control policy 
    - Resource Granularity: individual, asset type, domain (or a combination)
    - Action Granularity: READ, WRITE
    against an individual or group of DataHub resources (entities, aspects, roles, policies)
2. Define a named role, which maps to the access control policy
3. Define a configurable mapping from an authenticated Actor (DataHub user, groups) to one or more roles

Within 15 minutes or less. (dynamic)

Notice that we've omitted inclusion of

- introducing a new resource type
- introducing a new action type 

in the same amount of time. These should, however, be possible to change in code given sufficient development time. (static)
  