> This doc is for older versions (v0.2.1 and before) of WhereHows. Please refer to [this](https://github.com/linkedin/WhereHows/blob/master/wherehows-etl/README.md) for the latest version.

Multiproducts are applications and libraries that are developed and released as a unit. The source codes of multiproducts are hosted using SCM systems. Source Control Management (SCM) systems such as git and svn contain useful information about projects, repos and owners. And these information can be used to determine and track the lineage and ownership of datasets and jobs. Here in the Multiproduct Metadata ETL job, we fetch the information about the products and repos and crawl for the owner information in ACL files. 

## Configuration
List of properties required for the ETL process:

| configuration key | description|
|---|---|
| multiproduct.service.url | multiproduct metastore url|
| git.url.prefix | gitorious/git repo url prefix |
| svn.url.prefix | svn/viewvc repo url prefix |
| git.project.metadata | local file location to store the git project info csv file |
| product.repo.metadata | local file location to store the product and repo info csv file |
| product.repo.owner | local file location to store the repo owner info csv file |

## Extract
Major related files: [MultiproductExtract.java](https://github.com/linkedin/WhereHows/blob/master/metadata-etl/src/main/java/metadata/etl/git/MultiproductExtract.java)

The extract process first finds all multiproducts from a centralized management system URL. The result Json are expended into a list of products. For the products that are hosted on gitorious, we can call the repo location on gitorious to get more info about the repo and the project that contains this repo. All the product information, with or without the additional repo info, is merged into product_repo CSV file. 

The next thing is to extract the owner information from ACL files inside each product repo. For each repo, whether on gitorious or svn, we do a two step search for all the acl information. First, we fetch the '/acl' folder under the repo home and parse for all acl file names. Then for each acl file name, we fetch the raw file and parse for owner names and the covered path in the repo. During the first step, the SCM will redirect the request to the latest version of the source code, so we record this version commit hash and use it in all the step two queries to save redirection time.

## Transform
No transform needed.

## Load
Major related files: [MultiproductLoad.py](https://github.com/linkedin/WhereHows/blob/master/metadata-etl/src/main/resources/jython/MultiproductLoad.py)

Load from CSV files into staging tables: stg_git_project, stg_product_repo and stg_repo_owner, then merge the repo owners with dataset owners.

In the merging process, we find the mapping of a repo to the generated dataset by looking for the mapping relations from the cfg_object_name_map table. Then the repo owner will be inserted or updated to the dataset_owner table.
