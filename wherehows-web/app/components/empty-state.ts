import Ember from 'ember';

const { Component } = Ember;

export default Component.extend({
  tagName: 'empty-state',

  classNames: ['empty-state'],

  /**
   * Default heading for the empty state component
   * @type {string}
   */
  heading: 'No data found'
});
