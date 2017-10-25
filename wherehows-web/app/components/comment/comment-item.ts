import Component from '@ember/component';
import ComputedProperty from '@ember/object/computed';
import { computed, getProperties, get, set } from '@ember/object';
import { run, schedule } from '@ember/runloop';
import { inject } from '@ember/service';
import { baseCommentEditorOptions } from 'wherehows-web/constants';
import { IDatasetComment } from 'wherehows-web/typings/api/datasets/comments';
import Notifications, { NotificationEvent } from 'wherehows-web/services/notifications';
import noop from 'wherehows-web/utils/noop';

export default Component.extend({
  tagName: 'li',

  classNames: ['comment-item', 'nacho-container'],

  /**
   * Reference to the application notifications Service
   * @type {ComputedProperty<Notifications>}
   */
  notifications: <ComputedProperty<Notifications>>inject(),

  /**
   * The comment currently being edited
   * @type {IDatasetComment}
   */
  comment: <IDatasetComment>{},

  /**
   * A copy of the comment in the editor before the user updates it
   * @type {IDatasetComment}
   */
  previousComment: <IDatasetComment>{},

  /**
   * Default handler to delete comment
   * @type {Function}
   */
  deleteComment: noop,

  /**
   * Default handler to update a comment
   * @type {Function}
   */
  updateComment: noop,

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
    //TODO: FIX use component context
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

      get(this, 'notifications').notify(NotificationEvent.confirm, {
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
      const { comment, previousComment } = getProperties(this, ['comment', 'previousComment']);

      // Ensure that we have a change in the comment text
      if (comment.text !== previousComment.text) {
        this.stopEditing();
        const updateComment = this.updateComment;
        updateComment(comment.id, comment);

        return;
      }

      get(this, 'notifications').notify(NotificationEvent.info, { content: 'Nothing seems to have been changed?' });
    }
  }
});
