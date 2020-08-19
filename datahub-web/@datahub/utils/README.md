# @datahub/utils

## Description

This contains a set of utility _functions_ & _types_, shareable and useful in a generic way **_across_** DataHub packages.

The goal here is not to serve as a 'bin' for code that may not fit into other packages and need to be shared, but to create a way to easily transport and host highly reusable and abstracted base functions that are independent of any application state.

**Functions should have**

- a well defined and thought out interface
- abide by the single responsibility principle, the open-closed principle
- be clearly named, unambiguously and adequately describe their use
- be properly namespaced
- be unit tested

Good examples of utility functions that makes sense to be included here would be

- an `Array` utility like groupBy that takes a collection of records and returns a map, keyed by values from records in the collection.
- a URL helper like `getAbsoluteUrl` that builds an absolute URL string from a relative URL.

Other non-function code snippets that make sense here would be base types.
A good example would be

- the generic type `ArrayElement<T>` where `ArrayElement` is `type ArrayElement<T extends Array<any>> = T[0]`

## Installation

```
ember install @datahub/utils
```

## Usage

```typescript
import { ArrayElement } from '@datahub/utils/types/array';

import { getAbsoluteUrl } from '@datahub/utils/url';
```

## Table of Contents

### Installation

- `mint checkout wherehows-frontend
- `cd datahub-web/@datahub/utils`
- `yarn install`

### Linting

- `yarn lint:hbs`
- `yarn lint:js`
- `yarn lint:js --fix`

### Running tests

- `ember test` – Runs the test suite on the current Ember version
- `ember test --server` – Runs the test suite in "watch mode"
- `ember try:each` – Runs the test suite against multiple Ember versions

### Running the dummy application

- `ember serve`
- Visit the dummy application at [http://localhost:4200](http://localhost:4200).

For more information on using ember-cli, visit [https://ember-cli.com/](https://ember-cli.com/).

## License

This project is licensed under the [Apache License](LICENSE.md).
