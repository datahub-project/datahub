import Component from '@ember/component';
import { set } from '@ember/object';

export default Component.extend({
  header: 'Request Submitted',
  content: 'Processing your request...',

  actions: {
    close() {
      set(this, 'isLoadSpinner', false);
    }
  }
});
