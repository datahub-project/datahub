import Component from '@ember/component';
import { get } from '@ember/object';
import { action } from 'ember-decorators/object';

enum Key {
  Escape = 27
}

export default class extends Component {
  containerClassNames = ['avatars-detail-modal'];

  /**
   * External action to close detail interface
   */
  onClose: () => void;

  /**
   * Handles key up event on interface
   * @param {KeyboardEvent} { key, which }
   */
  @action
  onKeyUp({ key, which }: KeyboardEvent) {
    // if escape key, close modal
    if (which === Key.Escape || key === 'Escape') {
      get(this, 'onClose')();
    }
  }
}
