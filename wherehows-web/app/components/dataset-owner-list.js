import Component from '@ember/component';
import { computed } from '@ember/object';

export default Component.extend({
  tagName: 'section',
  classNames: ['dataset-owner-list'],

  ownersEmailList: computed('owners', function() {
    // Reduce owner email to a string containing emails, each separated by comma
    return this.get('owners')
      .mapBy('email')
      .filter(email => email)
      .join(', ');
  })
});
