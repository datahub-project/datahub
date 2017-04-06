import Ember from 'ember';

export default Ember.Component.extend({
  tagName: 'section',
  classNames: ['dataset-owner-list'],

  ownersEmailList: Ember.computed('owners', function () {
    // Reduce owner email to a string containing emails, each separated by comma
    return this.get('owners').mapBy('email').filter(email => email).join(', ');
  })
});
