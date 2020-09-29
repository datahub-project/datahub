// IMPORTANT: This file is purposely `js`. Otherwise we will have unconsistent builds.
// see https://github.com/typed-ember/ember-cli-typescript/issues/724 and
// https://github.com/simplabs/ember-simple-auth/issues/1854
import ApplicationMainRoute from 'datahub-web/routes/application-main';

export default class ApplicationRoute extends ApplicationMainRoute {
  beforeModel(...args) {
    return super.beforeModel.apply(this, args);
  }

  afterModel(...args) {
    return super.afterModel.apply(this, args);
  }
}
