# Overview

Authorization specifies _what_ accesses an _authenticated_ user has within a system.
This section is all about how DataHub authorizes a given user/service that wants to interact with the system.

:::note

Authorization only makes sense in the context of an **Authenticated** DataHub deployment. To use DataHub's authorization features
please first make sure that the system has been configured from an authentication perspective as you intend.

:::

Once the identity of a user or service has been established, DataHub determines what accesses the authenticated request has.

This is done by checking what operation a given user/service wants to perform within DataHub & whether it is allowed to do so.
The set of operations that are allowed in DataHub are what we call **Policies**.

Policies specify fine-grain access control for _who_ can do _what_ to _which_ resources, for more details on the set of Policies that DataHub provides please see the [Policies Guide](../authorization/policies.md).
