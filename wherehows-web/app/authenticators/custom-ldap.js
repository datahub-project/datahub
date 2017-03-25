import Base from 'ember-simple-auth/authenticators/base';
import Ember from 'ember';

export default Base.extend({
  authenticate: (username, password) => Promise.resolve(Ember.$.ajax({
    method: 'POST',
    url: '/authenticate',
    contentType: 'application/json',
    data: JSON.stringify({username, password})
  })),

  restore() {
    return Promise.resolve();
  },

  // TODO: Remove request server invalidate session
  // as unfortunately server is stateful and will retain an open session
  // invalidate() {
  // return Promise.resolve();
  // }
})