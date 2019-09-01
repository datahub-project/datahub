'use strict';

module.exports = {
  description: 'Creates a new addon already configured with this setup',
  locals(_options) {
    return this.ui
      .prompt({
        type: 'list',
        name: 'destination',
        message: 'Choose destination package:',
        choices: [
          { name: '@datahub: Open source package', value: '@datahub' },
          { name: '@linkedin: Internal package', value: '@linkedin' }
        ]
      })
      .then(data => {
        return {
          destination: data.destination
        };
      });
  },

  isUpdate() {
    // always creates a new one.
    return false;
  },

  fileMapTokens: function(_options) {
    return {
      __template__(options) {
        return options.dasherizedModuleName;
      },
      __group__(options) {
        return options.locals.destination;
      }
    };
  }
};
