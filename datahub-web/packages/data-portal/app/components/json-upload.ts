import Component from '@ember/component';
import { action, computed } from '@ember/object';
import { classNames } from '@ember-decorators/component';

@classNames('nacho-uploader')
export default class JsonUpload extends Component {
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
  change({ target }: Event): void {
    const { files } = target as HTMLInputElement;
    files && files.length ? this.sendFileAsText(Array.from(files).shift()) : null;
  }

  /**
   * Caches a unique id for this instances HTML Element for file upload
   * @type {ComputedProperty<string>}
   * @memberof JsonUpload
   */
  @computed()
  get uploadInputId(): string {
    return `${this.elementId}-uploader`;
  }

  /**
   * Reads a file as a text string and passes the output to the closure action
   * @param {File} [fileBlob]
   * @memberof JsonUpload
   */
  sendFileAsText(fileBlob?: File): void {
    const { receiveJsonFile } = this;
    const reader = new FileReader();

    if (typeof receiveJsonFile === 'function' && fileBlob) {
      reader.onload = ({ target }: ProgressEvent & { target: FileReader }): unknown =>
        target && receiveJsonFile(String(target.result));
      reader.readAsText(fileBlob);
    }
  }

  /**
   * Proxies the user's click interaction on the styled upload button to the file input element
   * @memberof JsonUpload
   */
  @action
  onUpload(): void {
    const input = this.element.querySelector<HTMLInputElement>(`#${this.uploadInputId}`);
    input && input.click();
  }
}
