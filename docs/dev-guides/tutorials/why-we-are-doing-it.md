# Why We Are Doing It

## Why bother to do it programmatically? Why not UI?
Using programmatic ways to emit metadata to DataHub can provide a number of benefits over using a UI. One key advantage is automation, which can save time and effort by streamlining the process of emitting metadata. Additionally, programmatic ways of emitting metadata can offer greater flexibility and control over the metadata being emitted, enabling you to customize the metadata to meet your specific needs. 

## What are the main use-cases?
APIs offer a wide range of use cases when it comes to emitting metadata. 
Below are some of the example usages, but the possibilties are not limited to the following examples. 

### Basic Usage
* [Adding Tags](http://yoonhyejin.github.io/datahub-forked/docs/dev-guides/tutorials/adding-tags)
* [Adding Terms](http://yoonhyejin.github.io/datahub-forked/docs/dev-guides/tutorials/adding-terms)
* [Adding Ownership](http://yoonhyejin.github.io/datahub-forked/docs/dev-guides/tutorials/adding-onwerships)

### Advanced Usage 
* Adding Tags on Entities Based on Entity Type
* Ingesting Entity from CSV Files
* Adding Column-level Lineages

## Our APIs (TBD)
Datahub supports three APIs : GraphQL, SDKs and OpenAPI. Each method has different usage and format. 
Here's an overview of what each API can do. 

> Last Updated : Mar 3 2023

|                                       | GraphQL | SDK | OpenAPI |
|---------------------------------------|--------|---|---|
| Add Tags/Terms/Ownership to a column of a dataset | ✅      |||
| Add Tags/Terms/Ownership to a dataset | ✅      |||
| Create Dataset                        ||| ✅        |
| Delete Dataset                        ||| ✅  |
| Search Dataset                        ||| ✅  |


