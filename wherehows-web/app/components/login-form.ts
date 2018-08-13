import Component from '@ember/component';
import { assert } from '@ember/debug';
import { action } from '@ember-decorators/object';

export default class LoginForm extends Component {
  classNames = ['nacho-login-form'];

  /**
   * External action to be invoked on form submission
   * @type {Function}
   * @memberof LoginForm
   */
  onSubmit: Function;

  constructor() {
    super(...arguments);

    // Ensure that the onSubmit action passed in on instantiation is a callable action
    const typeOfOnSubmit = typeof this.onSubmit;
    assert(
      `Expected action onSubmit to be an function (Ember action), got ${typeOfOnSubmit}`,
      typeOfOnSubmit === 'function'
    );
  }

  /**
   * Handle the login for submission
   */
  @action
  userDidSubmit() {
    // Trigger action on parent controller
    this.onSubmit();
  }
}
