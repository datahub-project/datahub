import Component from '@ember/component';
import { set } from '@ember/object';
import { action } from '@ember-decorators/object';

export default class UpstreamDataset extends Component {
  tagName = 'section';

  classNames = ['upstream-downstream-retention'];

  /**
   * Flag indicating the component is in view only mode and not editable
   * @type {boolean}
   * @memberof UpstreamDataset
   */
  isReadOnly: boolean = true;

  /**
   * Action handler to set component in edit mode
   * @memberof UpstreamDataset
   */
  @action
  onEdit() {
    set(this, 'isReadOnly', false);
  }
}
