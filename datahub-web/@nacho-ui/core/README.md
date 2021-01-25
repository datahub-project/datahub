# @nacho-ui/core

This addon is a core addon for host applications that are consuming @nacho-ui/_ components or @wherehows/_ components. It's purpose is to provide a core for shared functionality that these other addons consume. You can also potentially use the functions provided here to make styling the rest of your applications with CSS an easier experience.

Note: This addon is currently a work in progress with the Wherehows 3.0 and nacho open source component initiative. Based on the requirements, this addon is subject to significant changes.

## Installation

```
ember install @nacho-ui/core
```

## Usage

### CSS

For most addons, the CSS functionality should be exposed in your host application:

```
// app.scss

// Use this if you want to include a custom theme for your application. Include a file in the root of your sytles folder called
// 'nacho-core-theme' and import it before `nacho-core`. A sample can be found in this addon under
// `app/styles/nacho-core-theme-example.scss`
@import 'nacho-core-theme';
@import 'nacho-core';

...
// All other @nacho-ui or @wherehows imports should come after nacho-core
```

For your troubles, this addon provides some convenient functionality for your css needs:

**@function item-spacing(\$spacing-unit)**

item-spacing maps, defines the spaces between elements in application layout. Provides consistent spacing parameters throughout application vs arbitrary decisions on the developer

Params:

| Name         | Type   | Description                                 |
| ------------ | ------ | ------------------------------------------- |
| spacing-unit | number | Maps to our theme's scale for space mapping |

**@function color-palette(\$paletteKey)**

If you've inserted a theme for your host application, or want to use the predefined theme provided by nacho-core, you can use this function to quickly retrieve the theme colors without having to memorize compliacted color keys that aren't named in any memorable way. This comes with the added benefit of making sure you stick to your application's look and feel.

Params:

| Name       | Type   | Description                                               |
| ---------- | ------ | --------------------------------------------------------- |
| paletteKey | string | Maps to a named theme component. See example usages below |

Example Usage:

```
.my-button {
  background-color: color-palette(primary);
  border: color-palette(secondary);

  &:hover {
    background-color: color-palette(on-primary);
  }

  &--error-state {
    background-color: color-palette(error);
  }
}
```

**@function get-color($colorKey, $opacity)**

get-color maps to a specific color in your determined color bin (defined below), and opacity determines % visibility. This is a quick function to create an `rgba(x, y, z, #)` in your CSS. This picks from all the colors you want to make available in your application, which should include anything in your theme and then some.

Params:

| Name     | Type   | Description                                                                       |
| -------- | ------ | --------------------------------------------------------------------------------- |
| colorKey | string | Named color key in the set theme for your host application                        |
| opacity  | number | Default: 1. Number between 0 and 1 that should determine your color's CSS opacity |

**@mixin nacho-container**

Gives your container `div` elements a stylized look and feel according to your theming.

## Contributing to the Overall Nacho Library

### Installation

Currently, the Nacho component library is in a private repository while we finalize open source requirements. In the meantime, access will be given interally.

Once you have access, you can clone the repository from the link as a mono-repo. In the `./packages` folder you will find all of the individual Nacho addons that have been created, including `nacho-core`.

### What is the Philosophy of Nacho?

Before contributing to Nacho, it is important to understand why it even exists. The drive behind starting the Nacho components at LinkedIn started with the realization that our data applications were using similar designs and components. Buttons and their various states have a consistent look and feel, tables, pills, etc. Beyond that, even sets of components, what we are calling _macro components_ or _gadgets_ have their own consistent layout and look and feel as well. Forms, app headers, search pages between applications have common elements.

Therefore, it is beneficial that we can create a library that lets us reuse components, or easily extend them, to meet these common use cases. Not only will this save engineering time, but also lets us easily maintain a similar user experience between many applications.

### Why an Open Source Nacho?

This is also driven by a LinkedIn use case, internally at LinkedIn we have many in-house applications, but also have a number of open source products as well. Some proprietary information for applications at LinkedIn cannot be exposed to the open source world. Having these components be generalized for open source allows both internal applications and open sourced applications used internally to maintain a similar experience and benefit.

### Ember Components

All of the Nacho library uses Ember components. Therefore, it is important to know Ember conventions when developing. Some resources are

- [The main Ember website](https://guides.emberjs.com/release/) provides great resources to get started
- This [Frontend Masters Course](https://frontendmasters.com/courses/ember-2/) will also be a great foundational resource
- It'll be important to understand [Ember Addons](https://gist.github.com/kristianmandrup/ae3174217f68a6a51ed5) as well

### Typescript

Unless you only want to deal with the CSS side of the Nacho library, developing components will also require knowledge of [Typescript](https://www.typescriptlang.org/) and how it [works with Ember](https://github.com/typed-ember/ember-cli-typescript)

### I know Ember, now I want to create a Component!

Great! The first thing to do when you want to create a component is to find a home for it! We already have a number of existing addons, each dedicated to fulfilling a need. This can be very specific, such as `@nacho-ui/core` focuses on just buttons and their various states, while some can be more broad such as `@nacho-ui/search` handling multiple components related to search and lookup, and even making a full search macro-component.

### Creating an Addon

If no current Nacho addon fits as a home for your component, then it's time to create a new one! In the directory with the current Nacho packages:

```sh
ember addon <addon-name>
cd <addon-name>
rm -rf .git
```

This should really be a runnable script, but for now, we need to also install some basic dependencies. Most
Nacho components need at least:

```
ember install ember-cli-typescript ember-cli-sass ember-decorators
yarn add node-sass
```

### Logistics

In order to properly set up your addon, there are some minor logistics to take care of. First of all, there is a 99.87% chance that your nacho-addon should require `@nacho-ui/core`. Also, if you are creating a component, ember mandates that you move `ember-cli-html-bars` to your dependencies vs devDependencies. To add `@nacho-ui/core` to your project, simply run:

```sh
yarn add @nacho-ui/core
```

and after moving `ember-cli-html-bars` to being a dependency, in your `package.json` you should see:

```json
{
  "dependencies": {
    "ember-cli-html-bars": "semvers",
    "@nacho-ui/core": "semvers"
  }
}
```

Note that we are using [Yarn Workspaces](https://yarnpkg.com/lang/en/docs/workspaces/) so using yarn to add `@nacho-ui/core` actually linked to the same addon within the repo.

### I just want to create my Component already...

You're in luck! Creating a component is as easy as:

```
ember g component <your-component-name>
```

BUT...

### Component Naming Convention

Our components should all have a consistent naming philosophy: `nacho-<componentName>`

_Example_

```
nacho-button
nacho-table
nacho-pwr-lookup
```

A component name should generally not need to be longer than three-dasherized-words. In cases where the name is much longer, then either train harder in the ways of naming or consider maybe it's because this component actually belongs to a very tightly coupled group of components. In that case, it might be a good idea to group them under a directory:

_Example_

```
nacho-animations/pendulum-ellipsis-item
nacho-table/expanded-row-header
```

PS In case you didn't read this section before running the generate command above:

```
ember d component <component-name-to-be-deleted>
```

### Component Code Conventions

```javascript
@tagName('tag')
@classNames('classNamesHere')
export default class MyComponent extends Component {
  // This is the general order that class properties should appear for greater understandability:
  CONSTPROPS = 'Those values that never change';

  passedInPrimitives: TypingInformation;
  passedInObjectValues: { ObjectInterfaceDef };
  passedInFunctions: () => InterfaceTyping;

  classPrimitiveValues: TypingInformation = 'value' || 0 || false;
  classObjectValues: { InterfaceTyping } = {} || [] || new ConstructorClass()
  classMethods(): ReturnTyping {
    // Do something here
  }

  constructor() {
    super(...arguments);
    // Do other things, maybe setup some default values
  }

  // Ember Component Hooks Here
  didInsertElement() {}
  didUpdateAttrs() {}
  click() {}

  // MyComponent Actions here
  @action
  someAction(): ReturnTyping {}
}
```

### Documentation

For Nacho Components, we use JSDocs style documentation. Documentation should tell us what the component does in terms of user interface, how the component may interact with other components, and what is happening with properties and methods.

@Example:

```javascript
/**
 * MyComponent is an awesome component that gives the user insight into the future. It sits inside the
 * BiggerComponent component and is meant to be the window to their soul.
 *
 * @example
 * {{MyComponent
 *   numYears=25
 *   useVoodoo=true
 * }}
 */
export default class MyComponent extends Component {
  // ... Other stuff

  /**
   * This property determines whether or not the component has focus, and is triggered by the onClick Action
   * Note, we should always include type for these fields
   * @type {boolean}
   * @default false
   */
  isFocused: boolean = false;

  /**
   * A method should always have a types for the parameters along with the description.
   * This method is clicked by the user on the red box and then triggers a change in our isFocused property
   * @param {boolean} triggerState - the triggered state? lol
   * @returns {void}
   */
  windowClicked(triggerState: boolean): void {
    set(this, 'isFocused', triggerState);
  }
}
```

### Testing

The more testing the better! At the very least, components should be tested with rendering integration tests, interaction integration tests, and the dummy app should have a basic demonstration of the component working in a live environment.

- A guide to testing in Ember can be found on the [Ember Homepage](https://guides.emberjs.com/release/testing/)

To create a component to test out your stuff in the dummy app, run:

```
ember g component <test-component> --dummy
```

Then you can supply mock data or interact with the component in any way you want to demonstrate its use. To see it in all its glory, run in your project root:

```
ember s
```

## License

This project is licensed under the [Apache License](LICENSE.md).
