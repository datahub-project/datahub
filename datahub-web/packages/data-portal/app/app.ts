import Application from '@ember/application';
import Resolver from 'ember-resolver';
import loadInitializers from 'ember-load-initializers';
import config from './config/environment';

export default class App extends Application {
  modulePrefix = config.modulePrefix;
  podModulePrefix = config.podModulePrefix;
  Resolver = Resolver;
  init(): void {
    super.init();

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
}

loadInitializers(App, config.modulePrefix);
