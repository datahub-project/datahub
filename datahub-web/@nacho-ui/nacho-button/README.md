@nacho-ui/button
==============================================================================

Addon under the Nacho UI inititative to be a reusable component for various types of buttons

Installation
------------------------------------------------------------------------------

```
ember install @nacho-ui/button
```


Usage
------------------------------------------------------------------------------

### Nacho Toggle

The NachoToggle component is used when the user wants to switch between two states, and each state option is made
known to them by label before they actually click on the toggle. The different between this and a radio button or
checkbox is that those generally represent a `true` or `false` value on a single property, whereas we generally
would view a toggle as a switch between two entirely different values for a single property.

```html
{{nacho-toggle
  value=value
  leftOptionValue=leftValue
  leftOptionText=leftValue
  rightOptionValue=rightValue
  rightOptionText=rightValue
  onChange=(action "onChangeValue")
}}
```

### Nacho Sort

The NachoSortButton component is used when we want to display a button option to sort some list that has been
associated with the button

```html
{{nacho-sort-button
  isSorting=isSortingABoolean
  sortDirection=sortDirectionAString
  sortValue="pokemon"
  class="test-sort-button"
  baseClass="test-sort-button"
  onChange=(action "onChangeSortProperty")
}}
```

The sort button component also has a helper function that can be used by consuming components if you follow
a certain sorting convention and helps cycle through sorting scenarios so that you don't have to rewrite the
same logic over and over. Provide the component context, the key for isSorting and sortDirection and expect
mutation of these properties accordingly in the order of `no sort` => `sort ascending` => `sort descending`

```js
import { SortDirection, cycleSorting } from '@nacho-ui/button/components/nacho-sort-button';

export default class TestSortButton extends Component {
  isSorting = false;
  sortDirection = SortDirection.ASCEND;

  // ... Other codes

  @action
  onChangeSortProperty(): void {
    cycleSorting(this, 'isSorting', 'sortDirection');
  }
};
```


Contributing
------------------------------------------------------------------------------

### Installation

* `git clone <repository-url>`
* `cd nacho-button`
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
