import Component from '@ember/component';
import { get } from '@ember/object';
import { action } from '@ember/object';
import { Keyboard } from 'datahub-web/constants/keyboard';

export default class AvatarsDetail extends Component {
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
  onKeyUp({ key, which }: KeyboardEvent): void {
    // if escape key, close modal
    if (which === Keyboard.Escape || key === Keyboard[27]) {
      get(this, 'onClose')();
    }
  }
}
