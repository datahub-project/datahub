import Component from '@ember/component';
import { action } from '@ember/object';
import { classNames } from '@ember-decorators/component';
import { noop } from 'lodash';

@classNames('nacho-login-form')
export default class LoginForm extends Component {
  /**
   * External action to be invoked on form submission
   * @type {Function}
   * @memberof LoginForm
   */
  onSubmit: () => void = noop;

  /**
   * Handle the login for submission
   */
  @action
  userDidSubmit() {
    // Trigger action on parent controller
    this.onSubmit();
  }
}
