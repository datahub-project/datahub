import Component from '@glimmer/component';
import { action } from '@ember/object';

const baseClass = 'nacho-login-form';

export default class LoginForm extends Component<{
  /**
   * External action to be invoked on form submission
   * @type {Function}
   * @memberof LoginForm
   */
  onSubmit: () => void;
}> {
  baseClass = baseClass;

  /**
   * Handle the login for submission
   */
  @action
  userDidSubmit(ev: Event): void {
    // prevent browser to refresh the page
    ev.preventDefault();

    // Trigger action on parent controller
    this.args.onSubmit();
  }
}
