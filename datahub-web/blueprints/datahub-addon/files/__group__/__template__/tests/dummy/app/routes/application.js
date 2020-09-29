// IMPORTANT: This file is purposely `js`. Otherwise we will have unconsistent builds.
// see https://github.com/typed-ember/ember-cli-typescript/issues/724 and
// https://github.com/simplabs/ember-simple-auth/issues/1854
import ApplicationBaseRoute from '@datahub/shared/routes/application-base';
export default class ApplicationRoute extends ApplicationBaseRoute {
  beforeModel(...args) {
    return super.beforeModel.apply(this, args);
  }
}
