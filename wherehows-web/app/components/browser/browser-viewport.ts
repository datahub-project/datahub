import Component from '@ember/component';
import { action } from 'ember-decorators/object';

export default class extends Component {
  /**
   * Closure action to request more entities
   */
  getNextEntities: () => void;

  /**
   * Handles the request to fetch more entities
   */
  @action
  loadMoreEntities() {
    this.getNextEntities();
  }
}
