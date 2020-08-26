'use strict';

module.exports = {
  name: require('./package').name,
  isDevelopingAddon: () => true,

  included(app) {
    this._super.included(app);
    ['regular', 'italic', '200', '200italic', '300', '300italic', '600', '600italic'].forEach(weight =>
      ['eot', 'svg', 'ttf', 'woff', 'woff2'].forEach(extension =>
        app.import(`vendor/fonts/Source-Sans-Pro-${weight}/Source-Sans-Pro-${weight}.${extension}`, {
          destDir: `assets/fonts/Source-Sans-Pro-${weight}`
        })
      )
    );
  }
};
