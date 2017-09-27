import Ember from 'ember';

const { TextField } = Ember;

export default TextField.extend({
  /**
   * Prevents click event bubbling
   */
  click: () => false
});
