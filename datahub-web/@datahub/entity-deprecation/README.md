# entity-deprecation

The entity-deprecation addon was made as a table to handle the deprecation scenario for entities on
DataHub, but can also be incorporated into general deprecation scenarios.

### Example usage:
```
// container.hbs
{{datahub/entity-deprecation
  entityName="Pokemon"
  deprecated=boolDeprecated
  deprecationNote=stringDeprecationNote
  decommissionTime=timestampDecommissionTime
  entityDecommissionWikiLink=hashWikiLinks.entityDecommission
  onUpdateDeprecation=(action functionOrActionUpdateDeprecation)}}
```

## Setup

In order to use this application, import it to the host application and then include the following
line in the host application's app.scss:
```
// app.scss

// If you're not already using it in your application, be sure to include these imports before
// entity-deprecation import
@import 'ember-power-calendar';

... // other imports
@import 'entity-deprecation';
...
```

## Publishing to NPM

During development, our addon's `index.js` file looks like this:
```
module.exports = {
  name: '@datahub/entity-deprecation',
  isDevelopingAddon: () => true,
  ...
};
```

Make sure to modify `isDevelopingAddon` to return false before publishing to npm

## Development & Installation

* `git clone <repository-url>` this repository
* `cd entity-deprecation`
* `npm install`

## Running

* `ember serve`
* Visit your app at [http://localhost:4200](http://localhost:4200).

## Running Tests

* `npm test` (Runs `ember try:each` to test your addon against multiple Ember versions)
* `ember test`
* `ember test --server`

## Building

* `ember build`

For more information on using ember-cli, visit [https://ember-cli.com/](https://ember-cli.com/).
