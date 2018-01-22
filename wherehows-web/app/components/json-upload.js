import Ember from 'ember';

import Component from '@ember/component';
import { get, computed } from '@ember/object';

export default Component.extend({
  classNames: ['nacho-uploader'],

  /**
   * Handles the change event on the json-upload DOM element
   * @param {FileList} files extracts a FileList object from the event object
   * @return {void|null}
   */
  change({ target: { files = [] } }) {
    const hasFiles = !!files.length;
    if (hasFiles) {
      return this.sendFileAsText(Array.from(files).shift());
    }

    return null;
  },

  /**
   * Caches a unique id for the HTML Element for file upload
   */
  uploadInputId: computed(function() {
    return `${get(this, 'elementId')}-uploader`;
  }),

  /**
   * Reads a file as a text string and passes the output to the closure action
   * @param {Blob} fileBlob
   */
  sendFileAsText(fileBlob) {
    const closureAction = get(this, 'attrs.receiveJsonFile');
    const reader = new FileReader();
    if (typeof closureAction === 'function') {
      reader.onload = ({ target: { result } }) => closureAction(result);
      reader.readAsText(fileBlob);
    }
  },

  actions: {
    /**
     * Proxies the user's click interaction on the styled upload button to the file input element
     */
    didSelectUpload() {
      const uploadInput = this.$(`#${get(this, 'uploadInputId')}`);
      return uploadInput.click();
    }
  }
});
