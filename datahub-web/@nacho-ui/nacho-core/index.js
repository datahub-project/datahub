'use strict';

module.exports = {
  name: require('./package').name,
  isDevelopingAddon: () => true,

  included(app) {
    this._super.included(app);

    [
      'Black',
      'BlackItalic',
      'Bold',
      'BoldItalic',
      'Italic',
      'Light',
      'LightItalic',
      'Regular',
      'Thin',
      'ThinItalic'
    ].forEach(weight =>
      ['ttf'].forEach(extension =>
        app.import(`vendor/fonts/Lato/Lato-${weight}.${extension}`, {
          destDir: `assets/fonts/Lato`
        })
      )
    );
  }
};
