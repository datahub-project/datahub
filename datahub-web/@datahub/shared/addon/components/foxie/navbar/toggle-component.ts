import Component from '@glimmer/component';
import { inject as service } from '@ember/service';
import { alias } from '@ember/object/computed';
import FoxieService from '@datahub/shared/services/foxie';
import { IConfigurator } from '@datahub/shared/types/configurator/configurator';

export default class FoxieNavbarToggleComponent extends Component<{}> {
  /**
   * Injected service for our virtual assistant
   */
  @service
  foxie!: FoxieService;

  /**
   * Injected service for our configurator to access the user entity props
   */
  @service
  configurator!: IConfigurator;

  get showVirtualAssistant(): boolean {
    // Note: Remove hardcoded true once we have flag guards in place
    return Boolean(this.configurator.getConfig('showFoxie', { useDefault: true, default: true }));
  }

  @alias('foxie.isActive')
  isVirtualAssistantActive!: boolean;

  @alias('foxie.toggleFoxieActiveState')
  toggleVirtualAssistant!: Function;
}
