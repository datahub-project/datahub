# @datahub/datasets-core

"The handler for datasets as an entity under DataHub"

## Installation

```
ember install @datahub/datasets-core
```

## Usage & Development

This addon is meant to be used within the context of the DataHub application and exists to house our dataset as an entity
related components and utilities.

### Requirements

The following styling requirements should be met in order to apply stylings for `datasets-core`:

```scss
// Host application app.scss
@import 'nacho-table';
@import 'nacho-pill';

@import 'datasets-core';

... your code
```

### Dataset Compliance Annotations Table

The dataset compliance annotations will be broken down into a scalable and palatable set of components that each perform a
specific function.

General outline of the table overview is here:

```html
<!-- container: datasets/core/containers/compliance-page -->
<!-- container: datasets/core/tables/compliance-annotations -->
<!-- nacho/table -->
<table>
  <!-- datasets/core/tables/compliance-annotations/header -->
  <thead>
    Title...sorting...filters
  </thead>
  <!-- datasets/core/tables/compliance-annotations/body -->
  <tbody>
    <!-- datasets/core/tables/compliance-annotations/row -->
    <tr>
      <td>{{Field Name}}</td>
      <!-- datasets/core/tables/compliance-annotations/tags-cell
           Useful because there's too much logic that goes into this tag already -->
      <td>
        <div>{{tag}}</div>
        <!-- Edit mode with datasets/core/tables/compliance-annotations/annotation-dropdown
             Useful because this tag component is already complex enough on its own -->
        <div>Dropdown menu for annotation tagging</div>
      </td>
      <td>{{Status}}</td>
    </tr>
  </tbody>
</table>
```

## Contributing

### Installation

- `git clone <repository-url>`
- `cd datasets-core`
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
