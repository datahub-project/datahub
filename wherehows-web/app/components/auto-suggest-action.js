import Ember from 'ember';

const { Component, computed } = Ember;

export default Component.extend({
  tagName: 'button',

  classNames: ['compliance-auto-suggester-action'],

  classNameBindings: ['isAffirmative:compliance-auto-suggester-action--accept'],

  /**
   * Determines the type of suggestion action this is
   * if type property is passed in
   */
  isAffirmative: computed.equal('type', 'accept')
});
