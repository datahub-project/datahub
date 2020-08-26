# @datahub/metadata-types

This package holds metadata type definitions for metadata models, mid tier shapes, etc

## Installation

```
ember install @datahub/metadata-types
```

## Usage

Once installed you may include types for the namespace that you are interested in consuming in your application.

```ts
import { IDatasetApiView } from '@datahub/metadata-types/entity/dataset';
```

## Contributing

####Folder Layout
The folder structure is laid out similarly to how psdc models are namespaced.
This is to aid in familiarity and ease of mental mapping between the TypeScript types defined here and the respective MP models.

Please adhere to similar namespace and path structure when creating new types that are representative of pdsc models.

For cases where a type is needed, for example, an alias for convenience, that is not a corollary with a MP defined model, these types should be defined in local-types if there is no js emit.

For example, if you are creating a utility in `utils` that's annotated with a type alias, rather than defining the type in `@datahub/metadata-types/types` folder which can overtime lead to pollution, it may be preferable to define such a type in `@datahub/metadata-types/local-types`.

As mentioned previously, and for emphasis, `@datahub/metadata-types/types` should be reserved for types the directly mirror api types.

### Installation

- `git clone <repository-url>`
- `cd @datahub/metadata-types`
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
