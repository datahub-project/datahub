import CurrentUser from '@datahub/shared/services/current-user';
import UnifiedTracking from '@datahub/shared/services/unified-tracking';
import Component from '@ember/component';
import { action } from '@ember/object';
import { tagName, classNames } from '@ember-decorators/component';
import { inject as service } from '@ember/service';

@tagName('section')
@classNames('nacho-hero')
export default class HeroContainer extends Component {
  @service('unified-tracking')
  tracking: UnifiedTracking;

  @service('current-user')
  sessionUser: CurrentUser;

  /**
   * Handler to track the change of search selection entity
   */
  @action
  handleEntityChange(): void {}
}
