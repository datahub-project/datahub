# What is a metadata aspect?

A metadata aspect is a structured document, or more precisely a `record` in [PDL](https://linkedin.github.io/rest.li/pdl_schema),
 that represents a specific kind of metadata (e.g. ownership, schema, statistics, upstreams). 
 A metadata aspect on its own has no meaning (e.g. ownership for what?) and must be associated with a particular entity (e.g. ownership for PageViewEvent). 
 We purposely not to impose any model requirement on metadata aspects, as each aspect is expected to differ significantly.
 
Metadata aspects are immutable by design, i.e. every change to a particular aspect results in a [new version](../advanced/aspect-versioning.md) created. 
An optional retention policy can be applied such that X number of most recent versions will be retained after each update. 
Setting X to 1 effectively means the metadata aspect is non-versioned. 
It is also possible to apply the retention based on time, e.g. only keeps the metadata changes from the past 30 days.

While a metadata aspect can be arbitrary complex document with multiple levels of nesting, it is sometimes desirable to break a monolithic aspect into smaller independent aspects. 
This will provide the benefits of:
1. **Faster read/write**: As metadata aspects are immutable, every "update" will lead to the writing the entire large aspect back to the underlying data store. 
Likewise, readers will need to retrieve the entire aspect even if it’s only interested in a small part of it.
2. **Ability to independently version different aspects**: For example, one may like to get the change history of all the "ownership metadata" independent of the changes made to "schema metadata" for a dataset.
3. **Help with rest.li endpoint modeling**: While it’s not required to have 1:1 mapping between rest.li endpoints and metadata aspects, 
it’d follow this pattern naturally, which means one will end up with smaller, more modular, endpoints instead of giant ones.

Here’s an example metadata aspect. Note that the `admin` and `members` fields are implicitly conveying a relationship between `Group` entity & `User` entity. 
It’s very natural to save such relationships as URNs in a metadata aspect. 
The [relationship](relationship.md) section explains how this relationship can be explicitly extracted and modelled.

```
namespace com.linkedin.group

import com.linkedin.common.AuditStamp
import com.linkedin.common.CorpuserUrn

/**
 * The membership metadata for a group
 */
record Membership {

  /** Audit stamp for the last change */
  auditStamp: AuditStamp

  /** Admin of the group */
  admin: CorpuserUrn

  /** Members of the group, ordered in descending importance */
  members: array[CorpuserUrn]
}
```
