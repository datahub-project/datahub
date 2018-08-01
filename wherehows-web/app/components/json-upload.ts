import Component from '@ember/component';
import { get, computed } from '@ember/object';
import ComputedProperty from '@ember/object/computed';
import { action } from '@ember-decorators/object';

export default class JsonUpload extends Component {
  classNames = ['nacho-uploader'];

  /**
   * External action to receive json text resource
   * @memberof JsonUpload
   */
  receiveJsonFile: (textBlob: string) => void;

  /**
   * Handles the change event on the json-upload DOM element
   * @param {FileList} files extracts a FileList object from the event object
   * @return {void | null}
   */
  change({ target }: Event) {
    const { files } = <HTMLInputElement>target;
    return files && files.length ? this.sendFileAsText(Array.from(files).shift()) : null;
  }

  /**
   * Caches a unique id for this instances HTML Element for file upload
   * @type {ComputedProperty<string>}
   * @memberof JsonUpload
   */
  uploadInputId: ComputedProperty<string> = computed(function() {
    return `${get(this, 'elementId')}-uploader`;
  });

  /**
   * Reads a file as a text string and passes the output to the closure action
   * @param {File} [fileBlob]
   * @memberof JsonUpload
   */
  sendFileAsText(fileBlob?: File) {
    const closureAction = get(this, 'receiveJsonFile');
    const reader = new FileReader();

    if (typeof closureAction === 'function' && fileBlob) {
      reader.onload = ({ target }: ProgressEvent & { target: FileReader }) =>
        target && closureAction(String(target.result));
      reader.readAsText(fileBlob);
    }
  }

  /**
   * Proxies the user's click interaction on the styled upload button to the file input element
   * @returns {JQuery<HTMLElement>}
   * @memberof JsonUpload
   */
  @action
  onUpload(): JQuery<HTMLElement> {
    return this.$(`#${get(this, 'uploadInputId')}`).click();
  }
}
