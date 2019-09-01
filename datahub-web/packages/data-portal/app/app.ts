import Application from '@ember/application';
import Resolver from './resolver';
import loadInitializers from 'ember-load-initializers';
import config from './config/environment';

const App = Application.extend({
  Resolver,

  modulePrefix: config.modulePrefix,

  podModulePrefix: config.podModulePrefix,

  init() {
    this._super(...arguments);

    /**
     * disable touch events in ember's event dispatcher (Chrome scrolling fix)
     * @type {{touchstart: null, touchmove: null, touchend: null, touchcancel: null}}
     * @link https://github.com/TryGhost/Ghost-Admin/commit/0affec39320f85c94ab54e85fb8bb4103ef41947
     */
    this.customEvents = {
      touchstart: null,
      touchmove: null,
      touchend: null,
      touchcancel: null
    };
  }
});

loadInitializers(App, config.modulePrefix);

export default App;
