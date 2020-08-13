import Component from '@glimmer/component';
import { action } from '@ember/object';
import { AuthenticationType } from '@datahub/shared/constants/authentication/auth-type';

/**
 * Parameters for login button component
 */
interface ISsoLoginButtonArgs {
  /**
   * Action fn that will be invoked when user clicks on the button
   * @param authType We will always send SSO for this Auth
   */
  authenticateUser?(authType: AuthenticationType): Promise<void>;
}

/**
 * Login Button for SSO. It will send AuthenticationType.Sso to the authenticateUser fn.
 */
export default class SsoLoginButton extends Component<ISsoLoginButtonArgs> {
  /**
   * Handle fn when user clicks on login
   */
  @action
  handleLogin(): Promise<void> {
    const { authenticateUser = (): Promise<void> => Promise.reject() } = this.args;

    return authenticateUser(AuthenticationType.Sso);
  }
}
