@nacho-ui/pill
==============================================================================

The Nacho pill component creates an interactive element that can be added, dismissed, toggled, or defined by user-generated content.

Installation
------------------------------------------------------------------------------

```
ember install @nacho-ui/pill
```


Usage
------------------------------------------------------------------------------

#### NachoPillInput

The Nacho pill input component is used when you want to allow input for a set of tags that appear
as a pill-like item.

Expected behavior:
- If the tag already has a value, clicking on the X will trigger a function with the intention
  to delete the tag
- If the tag is an empty tag, clicking on the + will put us in "editing" mode to add a value
- While in editing mode, clicking the + again or pressing the enter key will create a new tag
- While in editing mode, pressing the tab key will create a new tag and also leave us in
  editing mode still to quickly add more tags
- While in editing mode, clicking away will cause the input pill to reset

Params:

| Name | Type | Description |
| ---- | ---- | ----------- |
| value | `string | undefined` | If the tag is simply to state a value that can be deleted, that goes here |
| placeholder | `string | undefined` | If the tag is ready for input, you can add a prompt message here |
| onComplete | `(p: string) => void` | Triggers the completion task for adding a tag, where p is the value of the user's text input |
| onDelete | `(p: string) => void` | Triggers the deletion of the tag, where P is the value of the `value` passed into the component |
| baseState | `PillState as string` | Allows the user to specify a custom state for the tag (defaults to `PillState.NeutralInverse`) when a value is displayed |
| emptyState | `PillState as string` | Allows the user to specify a custom state for the tag (defaults to `PillState.Inverse`) when prompting to enter a value  |


##### Example usage

```hbs
<NachoPillInput
  @value="stringOrUndefined"
  @placeholder={{"string" || undefined}}
  @onComplete={{action onComplete}}
  @onDelete={{action onDelete}}
  @baseState={{PillState.Good}}
  @emptyState={{PillState.GoodInverse}}
/>
```

```hbs
{{#each this.tagList as |tag|}}
  <NachoPillInput @value={{tag}} @onDelete={{action this.removeTag tag}}/>
{{/each}}
<NachoPillInput @placeholder="Add Pokemon" @onComplete={{action this.addTag}}/>
```

Contributing
------------------------------------------------------------------------------

### Installation

* `git clone <repository-url>`
* `cd nacho-pill`
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
