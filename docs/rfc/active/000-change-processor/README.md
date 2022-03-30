- Start Date: 2021-11-04
- RFC PR:
- Implementation PR(s):

# Change processing

## Summary

DataHub is designed to collate externally managed metadata, as such it doesn't impose any rules on metadata changes
(besides minimum data requirements to create entities/aspects). To start using DataHub for orchestration purposes we
require the ability to flexibly validate/modify a change before it is applied.

## Motivation

We're looking to replace an in-house Data Catalog with DataHub. The catalog we're replacing has the ability to
reject/ignore metadata changes based on a set of validation rules. The expected outcome would be that we would be able
to specify rules that different entities/aspects would be required to follow such that we aren't required to fork
DataHub.

## Requirements

- Validate metadata changes before they are applied based on properties of the incoming aspect change proposal. We
  require the ability to ignore certain changes based on the previous value.
- Easily create/change validation surrounding a metadata change.
- Not maintain a fork of DataHub. We could replicate our desired behaviour by just modifying DataHub in a fork, however
  one driver of replacing an internal catalog with DataHub is such that we no longer are required to maintain a service
  that handles orchestration logic.

## Detailed design

The metadata-io package currently has a
function ([updateLambda](https://github.com/linkedin/datahub/blob/352a0abf8d7e4dd5d5664a8c7cdf3d77bf6f1c51/metadata-io/src/main/java/com/linkedin/metadata/entity/ebean/EbeanEntityService.java#L236))
that is run before each aspect change proposal. It accepts the previous version of an aspect (or null if it's the first
version) and the proposed aspect change. It currently does nothing with this information and returns the proposed new
aspect value.

We could extend this behaviour such that users can choose to overload it.

### Proposed implementation

1. On metadata-service start up, use a ServiceLoader to load a jar from a specified location (defined in config). In the
   future this could be extended to load in jars at runtime.
2. The user defined jar would contain a class that implements a [interface](#contract-example) that the metadata service
   can understand.
3. Attach an annotation to the processor that specifies what entity/aspect the processor should apply to, and whether
   the processor is run before or after it's applied to the datastore.
4. On each update call pass the previous aspect version and the proposed new version into a processor that runs it
   through all registered processors.
5. If a validation fails bubble up the details to the caller.

If no custom jar is detected on start up keep the existing noop behaviour.

### Developer Lifecycle Example

Suppose we require the ability to ignore metadata updates where a property contains a specific string. The following
steps indicates the development lifecycle:

1. Create a new project that will contain the custom validation logic.
2. Add a dependency to the DataHub metadata-io project that contains the expected contracts.
3. Implement the class and write the change processors.
4. Build and deploy the jar to a location the running metadata-service will have access to.
5. Restart the metadata-service and provide the path to the new jar via config.
6. All new updates will pass through the new code path checking whether that aspect change contains the disallowed
   string.

### Contracts Example
[Change processor interface](../../../../metadata-io/src/main/java/com/linkedin/metadata/changeprocessor/ChangeProcessor.java): Interface that clients will develop their custom processor logic against. 

[Change processor annotation](../../../../metadata-io/src/main/java/com/linkedin/metadata/changeprocessor/ChangeProcessorScope.java): Allows processors to target specific entity aspect combinations.

[Change stream processor](../../../../metadata-io/src/main/java/com/linkedin/metadata/changeprocessor/ChangeStreamProcessor.java): Responsible for running all the registered processors

## Drawbacks

* Opens up ability to add unwanted side effects that DataHub cannot control. 
* Difficult to see what validation is being applied to what aspect at runtime.
* Deployment of DataHub would be more complicated than it is currently, before starting the metadata service the jar
  must exist in the expected location.

## Alternatives

* Modify DataHub to provide workflow functionality, where users can upload
  a [BPMN](https://en.wikipedia.org/wiki/Business_Process_Model_and_Notation) diagram into DataHub and offer similar
  functionality to workflow engines like Camunda. Downsides include introducing much more complexity into DataHub.

* Instead of loading validation logic by jars, create a validation service that DataHub calls out to. Requires DataHub
  clients to host and maintain a separate service. Also could impact performance by adding a separate http hop per
  update.

## Rollout / Adoption Strategy

Introducing the proposed design above won't be a breaking change because if no custom jars are found on start up then this
feature is ignored.


# UPDATE
As it stands the current proposal applies the change processor at a point in the {Specific}EntityService, and the recent changes mean there would need to be a number of locations the processors are applied in both Ebean and Cassandra.

In essence the proposal is to add a concept to the processing within the EntityService, which avoids changing things too much, so there is a common place where the interactions with the DAO and execution of processing steps. Ideally this would remove the {Specific}EntityService and flatten things making it easier to add new stores.

The specific Change Processors could be dynamically loaded from customer jars, but the specification of which processors are to be executed are stored in an aspect in the database. This ensures the ordering of the processors is not a runtime decision, but a configuration decision.
Also for our use case, the lack of a state machine step should terminate processing.

This context could be created and ensure the correct level of transaction is applied, it could also confirm the entire execution is completed and which proposed changes were accepted or rejected/ignored. 
If in the future a change processor takes a single input and generate multiple changes, these could be added to the accepted change set, with some traceability for the source if required.
The entire context success, partial or complete could be tracked allowing better visibility of failure of say Audit event publication.

This separation allows the entity service to reduce responsibility, and delegate the work to a pipeline of stages. The diagram tries to show how Proposed Changes could flow through the processing pipe, with fixed pre-processor check change and Audit event publication.


![image](https://user-images.githubusercontent.com/97735469/160853047-b2f15257-2bdf-4d29-990c-e68182415d48.png)
