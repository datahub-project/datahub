import Ember from 'ember';

const { Component, computed, getProperties } = Ember;

export default Component.extend({
  tagName: 'button',

  classNames: ['compliance-auto-suggester-action'],

  classNameBindings: ['isAffirmative:compliance-auto-suggester-action--accept'],

  /**
   * Determines the type of suggestion action this is
   * if type property is passed in
   */
  isAffirmative: computed.equal('type', 'accept'),

  /**
   * Action handler for click event, invokes closure action with type as argument
   */
  click() {
    const { type: intent, action } = <{ type: string | void; action: Function | void }>getProperties(
      this,
      'type',
      'action'
    );

    if (typeof action === 'function') {
      return action(intent);
    }
  }
});
