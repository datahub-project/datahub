import Ember from 'ember';
import { baseCommentEditorOptions } from 'wherehows-web/constants';
import { IDatasetComment } from 'wherehows-web/typings/api/datasets/comments';

const { Component, getProperties, get, set, inject: { service }, computed, run } = Ember;
const { schedule } = run;

export default Component.extend({
  tagName: 'li',

  classNames: ['comment-item', 'nacho-container'],

  /**
   * Reference to the application notifications Service
   * @type {Ember.Service}
   */
  notifications: service(),

  /**
   * Flag indicating the comment is in edit mode
   * @type {boolean}
   */
  isEditing: false,

  /**
   * Editor options to set on the comment editor
   * @type {object}
   */
  editorOptions: computed(() => baseCommentEditorOptions).readOnly(),

  /**
   * Applies focus to the comment editor
   */
  focusEditor(): void {
    this.$('.comment-new__content').focus();
  },

  /**
   * Sets the edit flag to false
   */
  stopEditing(): void {
    set(this, 'isEditing', false);
  },

  actions: {
    /**
     * Handles delete initiation by passing comment id upstream for deletion
     */
    async onDelete(): Promise<void> {
      const { id } = get(this, 'comment');
      const deleteComment = this.deleteComment;
      const dialogActions: { [prop: string]: () => void } = {};
      const confirmHandler = new Promise((resolve, reject) => {
        dialogActions['didConfirm'] = () => resolve();
        dialogActions['didDismiss'] = () => reject();
      });

      get(this, 'notifications').notify('confirm', {
        header: 'Delete comment',
        content: 'Are you sure you want to delete this comment?',
        dialogActions: dialogActions
      });

      try {
        await confirmHandler;
        deleteComment(id);
      } catch (e) {
        //no-op
      }
    },

    /**
     * Places the comment in an edit state
     */
    onEdit(): void {
      set(this, 'isEditing', true);
      set(this, 'previousComment', { ...get(this, 'comment') });

      run(() => {
        schedule('afterRender', this, 'focusEditor');
      });
    },

    /**
     * Cancels component out of edit more
     */
    onCancel(): void {
      this.stopEditing();
    },

    /**
     * Publishes the updated comment upstream
     */
    updateComment(): void {
      const { comment, previousComment } = <{
        comment: IDatasetComment;
        previousComment: IDatasetComment;
      }>getProperties(this, ['comment', 'previousComment']);

      // Ensure that we have a change in the comment text
      if (comment.text !== previousComment.text) {
        this.stopEditing();
        const updateComment = this.attrs.updateComment;
        updateComment(comment.id, comment);

        return;
      }

      get(this, 'notifications').notify('info', { content: 'Nothing seems to have been changed?' });
    }
  }
});
