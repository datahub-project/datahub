import Ember from 'ember';

/**
 * Funtion used to pause the test
 */
export function pause(timeout) {
  let promise = $.Deferred();
  Ember.run.later(function() {
    promise.resolve();
  }, timeout);
}
