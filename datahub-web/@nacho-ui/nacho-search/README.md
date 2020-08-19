@nacho-ui/search
==============================================================================

The `@nacho-ui/search` addon contains a variety of components and macro-components to fulfill the building blocks necessary
to create a unified and complete search experience.


Installation
------------------------------------------------------------------------------

```
ember install nacho-search
```


Usage
------------------------------------------------------------------------------

### nacho-pwr-lookup
The nacho-pwr-lookup is a small, no frills component that helps with simple search tasks such as a typeahead
lookup inside a table, where we need something simple instead of a fullblown search experience. We wrap around
ember-power-select with a basic interface, but the actual act of searching / handling results should be delegated
to the container component, or an extended one

Example Usage:
```html
{{nacho-pwr-lookup
  suggestionLimit=number
  searchPlaceholder=stringIsOptional
  searchResolver=functionOrActionInterfaceDefinedInClass
  confirmResult=functionOrActionInterfaceDefinedInClass
}}
```

Params:
| Name | Type | Description |
| ---- | ---- | ----------- |
| suggestionLimit | number | Default 10. The largest number of suggestions we want to show in the typeahead |
| searchPlaceholder | string | The placeholder we want in the input when user isn't focused |
| searchResolver | function | External action passed in as a handler required to perform the lookahead search for the power select component. Function definition is based on ember-power-select requirements. Most common use case of searchResolver would be in the body to async fetch results, then return asyncResultsCallback(results) |
| confirmResult | function | External action passed in as a handler required to confirm the result upon user action. |

Example for `searchResolver`:
```javascript
searchResolver(query, scb, asyncResults) {
  const ldapRegex = new RegExp(`^${userNameQuery}.*`, 'i');
  const { userEntitiesSource = [] } = await getUserEntities();
  asyncResults(userEntitiesSource.filter(entity => ldapRegex.test(entity)));
}
```

Example for `confirmResult`:
```javascript
confirmResult(userName) {
  if (!userName) return;
  const findUser = parentComponent.findUser;
  const userEntity = await findUser(userName);
  if (userEntity) ... do something;
}
```


Contributing
------------------------------------------------------------------------------

### Installation

* `git clone <repository-url>`
* `cd nacho-search`
* `yarn install`

### Linting

* `yarn lint:hbs`
* `yarn lint:js`
* `yarn lint:js --fix`

### Running tests

* `ember test` – Runs the test suite on the current Ember version
* `ember test --server` – Runs the test suite in "watch mode"
* `ember try:each` – Runs the test suite against multiple Ember versions

### Running the dummy application

* `ember serve`
* Visit the dummy application at [http://localhost:4200](http://localhost:4200).

For more information on using ember-cli, visit [https://ember-cli.com/](https://ember-cli.com/).

License
------------------------------------------------------------------------------

This project is licensed under the [Apache License](LICENSE.md).
