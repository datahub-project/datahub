@nacho-ui/table
==============================================================================

Tables are a vital part of displaying data, but every table implementation ends up reinventing the wheel.

This provides us with an opportunity as part of the Nacho components to rethink the table issue and come up with a solution that meets the needs of as many consumers as possible both internally and in the open source world. The goal of this documentation is to compare different approaches and ideas in order to arrive at the best solution possible.


Installation
------------------------------------------------------------------------------

```
ember install @nacho-ui/table
```

In order to use the stylings available for `{{nacho-table}}`, we need to make them available in our application. It is presumed that consuming applications are using Sass CSS Preprocessor for this

```scss
// app.scss
@import 'nacho-core';
@import 'nacho-table';
```

Usage
------------------------------------------------------------------------------

There are a couple of major approaches to how to consume the `{{nacho-table}}` component.

### Component Blocks

### Table Configs

This provides a way to get a basic table interface without having to type up a bunch of templating logic. An example of the minimal interface can be seen here:

```html
{{nacho-table
  data=rowData
  tableConfigs=myConfigs
}}
```

**tableConfigs Params**

| Name | Type | Description |
| ---- | ---- | ----------- |
| headers | `Array<NachoTableHeaderConfig>` | A list of objects that represents how we want to render the header compoments. |
| labels | `Array<string>` | Important! Defines a list of ids that will determine how the configuration maps to each column of your table. For a particular row and particular column, the default rendered value will be the label property on the row object as `rowObject[label]` |
| useBlocks | `{} as UseBlocks` | Helpful for when we want to yield certain parts of the table to template blocks but not others. See below at `useBlocks Params` for more |
| customRows | `NachoTableCustomRowConfig` | If we want to do a customization that affects every row, we might find the capability available here. |
| customColumns | `NachoTableCustomColumnConfig` | If we want to do a customization that affects every cell in a certain column for each row. The colum config object is an object whose keys are the same strings found in the `labels` array. Each key is then mapped to a `NachoTableCustomColumnConfig` below. |

**NachoTableHeaderConfig Params**

| Name | Type | Description |
| ---- | ---- | ----------- |
| title | `string` | Actual title to render in the header |
| className | `string` | Additonal class to give the header title cell. Helps additional customization. |
| component | `string` | Defaults `undefined`. Takes priority if exists, will render a specified component as the cell instead |

**useBlocks Params**

| Name | Type | Description |
| ---- | ---- | ----------- |
| header | `boolean` | Will defer to a block for the header, which means the table configurations will make no assumptions about the header from configurations |
| body | `boolean` | Will defer to a template block for the body, which means it makes no assumptions about the body from the tableConfigs |
| footer | `boolean` | Will defer to a template block for the footer |

**NachoTableCustomRowConfig Params**

| Name | Type | Description |
| ---- | ---- | ----------- |
| component | `string` | Will defer all rows to a specified component. The table will pass into this component the `tableConfig` and row object per row |
| className | `string` | Will apply a given class name to every row element in the table |

**NachoTableCustomColumnConfig Params**

| Name | Type | Description |
| ---- | ---- | ----------- |
| className | `string` | Will apply a given class name to every cell in the specified column |
| compoment | `string` | Path to a component to render instead fo the table default for every cell in a column. Properties passed to the component will be the `tableConfig`, the `rowData` for the whole row containing the cell, and the `field` which currently can only map to the `rowData[label]` but will be expanded in the future |
| displayLink | `DisplayLink` | A very common use case for a table cell is to display a text link. If this is the only transformation we need for our cell, then having the consumer need to pass in a whole custom component is overkill. Instead, they can use display link to compute the proper element attributes from their row data object |

**DisplayLink Params**
| Name | Type | Description |
| ---- | ---- | ----------- |
| className | `string` | Will apply a given class to the `<a>` tag rendered in the cell for a custom link |
| isNewTab | `boolean` | Whether the link should open in a new tab or redirect from the current one. Adds target="_blank" |
| compute | `function` | Params: `rowData` - The row object which contains the particular column. The consumer can use this row object to calculate the desired href and display name for their link to be applied to the rendered `<a>` tag. Returned object from this function should be a `{ ref: string, display: string }`,  where ref is the href of the link and display is an optional display. Display will default to `ref` if not supplied |


Contributing
------------------------------------------------------------------------------

### Installation

* `git clone <repository-url>`
* `cd nacho-table`
* `npm install`

### Linting

* `npm run lint:hbs`
* `npm run lint:js`
* `npm run lint:js -- --fix`

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
