import { TestContext } from 'ember-test-helpers';
import Helper from '@ember/component/helper';

export const setupHandlerRegistrations = function(hooks: NestedHooks): void {
  hooks.beforeEach(function(this: TestContext) {
    // TODO [META-12344] WithBannerOffset lives in data-portal
    this.owner.register(
      `helper:with-banner-offset`,
      class WithBannerOffset extends Helper {
        compute(): string {
          return '';
        }
      }
    );
  });
};
