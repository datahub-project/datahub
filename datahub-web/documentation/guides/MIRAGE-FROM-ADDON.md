Using mirage-addon
==============================================================================

This guide is intended to provide notes for using mirage-from-addon in the individual packages and the
potential gachas that may occur.

## Getting started

Ensure that you require configs from `configs/import-mirage-tree-from-addon` in your consumed addon.

```js
'use strict';

module.exports = {
  name: require('./package').name,

  isDevelopingAddon: () => true,

  ...require('../../configs/import-mirage-tree-from-addon')
};
```

Your folder structure needs to be the following:

```
addon-root
\_addon
    \_mirage-addon
        \_scenarios
            \_default.ts
        \_test-helpers
        \_config.ts
\_tests
    \_dummy
        \_mirage
            \_factories
            \_fixtures
            \_models
```

In short, the items from `mirage-addon` will be ultimately merged with `tests/dummy/mirage` inside
your addon for testing. These will also be importable to the host application

In the host application, we will import the factories like this:

```js
// data-portal/mirage/scenarios/default.ts
...
import dataConceptsScenario from '@linkedin/data-concept/mirage-from-addon/scenarios/data-concepts';
import topConsumersScenario from '@datahub/shared/mirage-addon/scenarios/top-consumers';

export default function(server: Server): void {
  dataConceptsScenario(server);
  topConsumersScenario(server);
  ...
}
```

And route handlers will be handled as:

```js
import { setup as setupSharedConfig } from '@datahub/shared/mirage-addon/mirage-config';
import { setupDataConceptsConfig } from '@linkedin/data-concept/mirage-from-addon/mirage-config';
import { setup as setupMetrics } from '@linkedin/metric/mirage-addon/mirage-config';

export default function(): void {
  const server = (this as unknown) as Server;

  ...

  setupSharedConfig(server);
  setupDataConceptsConfig(server);
  setupMetrics(server);
```

## Gachas

### Using fixtures

Fixtures has an issue being imported normally through this method. We have found a workaround as:

```js
import { Server } from 'ember-cli-mirage';
import dataConceptsFixture from '@linkedin/data-concept/mirage-from-addon/fixtures/data-concepts';

export default function(server: Server): void {
  // Note: Using loadData() as currently we are facing issues with loadFixtures() from addon
  server.db.loadData({ dataConcepts: dataConceptsFixture });
```

### Testing in an addon that consumes an addon using mirage-from-addon

This logic already exists in our host application, but to use the mirage factories from an addon
in another addon (for example, in an addon for an entity that is the internal counterpart to an
external addon) we need to modify the `ember-cli-build.js` of the consuming addon:

```js
const EmberAddon = require('ember-cli/lib/broccoli/ember-addon');

module.exports = function(defaults) {
  const app = new EmberAddon(defaults, {
    ...
    'mirage-from-addon': {
      includeAll: true,
      exclude: [/scenarios\/default/, /config/]
    }
  });

  ...

  return app.toTree();
};
```
