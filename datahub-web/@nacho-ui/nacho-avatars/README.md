nacho-avatars
==============================================================================

The Nacho Avatars addon is used for when we want to render a rounded image for a particular individual's avatar image.
We also provide macro components for viewing an avatar's details or creating stacked facepiles and rollups.

Installation
------------------------------------------------------------------------------

```
ember install @nacho-ui/avatars
```


Usage
------------------------------------------------------------------------------

### Getting Started

*Note*: This addon requires that your host application also includes the `@nacho-ui/core` addon, which provides some cores stylings and scss functions that are used throughout many nacho addons. It also requires that your application uses SCSS. Otherwise, the consuming application will have to write it's own look and feel

```scss
// <your host app>/styles/app.scss
@import 'nacho-core';
@import 'nacho-avatars';
...
```

In your application, you should at some point use a fallback URL if you plan on having a default avatar appear in cases where the url did not load.

```javascript
// This is just an example
import { inject as service } from '@ember/service';

export default class Application extends Route {
  nachoAvatarService = inject('nacho-avatars');
  ...
  constructor() {
    super(...arguments);
    this.get('nachoAvatarService').set('imgFallbackUrl', 'https://vignette.wikia.nocookie.net/starwars/images/2/20/LukeTLJ.jpg/revision/latest?cb=20170927034529');
  }
  ...
}
```

Then in your templates, you can try to use your avatar images

```html
<!-- your-template.hbs -->
<div>
  {{nacho-avatar
    img="cdn.linkedin.media/getimage/ash-ketchum.jpg"
    altText="Ash Ketchum"
  }}
  ...
</div>
```

Contributing
------------------------------------------------------------------------------

### Installation

* `git clone <repository-url>`
* `cd nacho-avatars`
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
