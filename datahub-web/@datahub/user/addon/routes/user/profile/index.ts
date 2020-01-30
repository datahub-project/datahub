import Route from '@ember/routing/route';
import { inject as service } from '@ember/service';
import DataModelsService from '@datahub/data-models/services/data-models';

export default class UserProfileIndexRoute extends Route {
  /**
   * Injection of data models service so that we can generically get the correct person entity
   * model class
   */
  @service('data-models')
  dataModels!: DataModelsService;

  /**
   * If we land on the index page (i.e. no tab given) for the person entity profile page, then we
   * can simply transition to the default tab
   */
  model(): void {
    const PersonEntityClass = this.dataModels.getModel('people');
    const personEntityPageRenderProps = PersonEntityClass.renderProps.entityPage;
    if (personEntityPageRenderProps) {
      this.transitionTo('user.profile.tab', personEntityPageRenderProps.defaultTab);
    }
  }
}
