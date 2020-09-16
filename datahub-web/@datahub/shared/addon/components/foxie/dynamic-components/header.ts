import Component from '@glimmer/component';
import { inject as service } from '@ember/service';
import CurrentUser from '@datahub/shared/services/current-user';

interface IFoxieDynamicComponentsHeaderArgs {
  options: {
    text: string;
  };
}

export default class FoxieDynamicComponentsHeader extends Component<IFoxieDynamicComponentsHeaderArgs> {
  static headerNamePlaceholder = '__USERNAME__';

  /**
   * Injects the current user so we can get their name and address them personally with the assistant
   */
  @service
  currentUser!: CurrentUser;

  /**
   * Attempts to get the name of the logged in user through various fallbacks, with the final default being to address
   * them as just "User"
   */
  get currentUserName(): string {
    const { entity } = this.currentUser;
    return (entity?.fullName || entity?.name || entity?.username || 'User').split(' ')[0];
  }

  get headerText(): string {
    return (this.args.options.text || '').replace(
      FoxieDynamicComponentsHeader.headerNamePlaceholder,
      this.currentUserName
    );
  }
}
