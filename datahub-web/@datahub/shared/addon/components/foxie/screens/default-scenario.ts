import Component from '@glimmer/component';
import { action } from '@ember/object';
import { tracked } from '@glimmer/tracking';

export default class FoxieScreensDefaultScenario extends Component<{}> {
  @tracked
  showWhatsNew = false;

  @action
  toggleWhatsNew(): void {
    this.showWhatsNew = true;
  }
}
