import Component from '@ember/component';
import { StreamCommentActionsUnion } from 'datahub-web/constants';
import { StringUnionKeyToValue } from 'datahub-web/typings/generic';
import { noop } from 'lodash';

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

  /**
   * Default no-op function to add a comment
   * @type {Function}
   */
  addCommentToStream: noop,

  /**
   * Default no-op function to delete a comment
   * @type {Function}
   */
  deleteCommentFromStream: noop,

  /**
   * Default no-op function to update a comment
   * @type {Function}
   */
  updateCommentInStream: noop,

  actions: {
    /**
     * Async handles CrUD operations for comment stream actions, proxies to parent closure actions
     * @param {StreamCommentActionsUnion} strategy
     * @return {Promise<boolean>}
     */
    handleStreamComment(strategy: StreamCommentActionsUnion, ...args: Array<unknown>): void {
      return {
        create: (): void => this.addCommentToStream(...args),
        destroy: (): void => this.deleteCommentFromStream(...args),
        update: (): void => this.updateCommentInStream(...args)
      }[strategy]();
    }
  }
});
