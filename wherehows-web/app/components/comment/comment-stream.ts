import Ember from 'ember';
import { StreamCommentActionsUnion } from 'wherehows-web/constants';
import { StringUnionKeyToValue } from 'wherehows-web/typings/generic';

const { Component, assert } = Ember;

/**
 * Actions available for comment stream
 * @type {CommentActions}
 */
const CommentActions: StringUnionKeyToValue<StreamCommentActionsUnion> = {
  create: 'create',
  update: 'update',
  destroy: 'destroy'
};

export default Component.extend({
  tagName: 'ul',

  classNames: ['comment-stream'],

  /**
   * Mapping of available comment action
   * @type {StringUnionKeyToValue<StreamCommentActionsUnion>}
   */
  commentActions: CommentActions,

  actions: {
    /**
     * Async handles CrUD operations for comment stream actions, proxies to parent closure actions
     * @param {StreamCommentActionsUnion} strategy
     * @return {Promise<boolean>}
     */
    async handleStreamComment(strategy: StreamCommentActionsUnion): Promise<boolean> {
      const [, ...args] = [...Array.from(arguments)];

      // assert that handler is in CommentAction needed since we are calling from component template
      // TS currently has no jurisdiction there
      assert(`Expected action to be one of ${Object.keys(CommentActions)}`, strategy in CommentActions);

      return {
        create: (): Promise<boolean> => this.attrs.addCommentToStream(...args),
        destroy: (): Promise<boolean> => this.attrs.deleteCommentFromStream(...args),
        update: (): Promise<boolean> => this.attrs.updateCommentInStream(...args)
      }[strategy]();
    }
  }
});
