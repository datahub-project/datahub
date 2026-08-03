## Overview

TIBCO ActiveMatrix BusinessWorks and TIBCO Cloud Integration are enterprise
integration platforms for building and running application integrations. Learn
more at the [TIBCO documentation site](https://docs.tibco.com/).

The DataHub integration captures the deployment topology of TIBCO integration
applications — deployment scopes and the applications deployed within them,
along with their version and run state.

## Concept Mapping

| Source Concept                            | DataHub Concept                                           | Notes                               |
| ----------------------------------------- | --------------------------------------------------------- | ----------------------------------- |
| `"tibco-bw"`                              | [Data Platform](../../metamodel/entities/dataPlatform.md) |                                     |
| Appspace (on-prem) / Subscription (cloud) | [Data Flow](../../metamodel/entities/dataFlow.md)         | Subtype `Appspace` / `Subscription` |
| Deployed application                      | [Data Job](../../metamodel/entities/dataJob.md)           | Subtype `Application`               |
