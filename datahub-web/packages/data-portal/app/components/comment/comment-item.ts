import Component from '@ember/component';
import { set } from '@ember/object';
import { run, schedule } from '@ember/runloop';
import { baseCommentEditorOptions } from 'datahub-web/constants';
import { IDatasetComment } from 'datahub-web/typings/api/datasets/comments';
import Notifications from '@datahub/utils/services/notifications';
import { noop } from 'lodash';
import { NotificationEvent } from '@datahub/utils/constants/notifications';
import { tagName, classNames } from '@ember-decorators/component';
import { inject as service } from '@ember/service';
import { action } from '@ember/object';
import { IConfirmOptions } from '@datahub/utils/types/notifications/service';

@tagName('li')
@classNames('comment-item', 'nacho-container')
export default class CommentItem extends Component {
  /**
   * Reference to the application notifications Service
   * @type {ComputedProperty<Notifications>}
   */
  @service
  notifications: Notifications;

  /**
   * The comment currently being edited
   * @type {IDatasetComment}
   */
  comment: IDatasetComment;

  /**
   * A copy of the comment in the editor before the user updates it
   * @type {IDatasetComment}
   */
  previousComment: IDatasetComment;

  /**
   * Default handler to delete comment
   * @type {Function}
   */
  deleteComment = noop;

  /**
   * Default handler to update a comment
   * @type {Function}
   */
  updateComment = noop;

  /**
   * Flag indicating the comment is in edit mode
   * @type {boolean}
   */
  isEditing = false;

  /**
   * Editor options to set on the comment editor
   * @type {object}
   */
  editorOptions = { ...baseCommentEditorOptions };

  /**
   * Applies focus to the comment editor
   */
  focusEditor(): void {
    const commentContent = this.element.querySelector<HTMLElement>('.comment-new__content');

    commentContent && commentContent.focus();
  }

  /**
   * Sets the edit flag to false
   */
  stopEditing(): void {
    set(this, 'isEditing', false);
  }

  /**
   * Handles delete initiation by passing comment id upstream for deletion
   */
  @action
  async onDelete(): Promise<void> {
    const { id } = this.comment;
    const deleteComment = this.deleteComment;
    const dialogActions: IConfirmOptions['dialogActions'] = {
      didConfirm: noop,
      didDismiss: noop
    };
    const confirmHandler = new Promise((resolve, reject): void => {
      dialogActions['didConfirm'] = (): unknown => resolve();
      dialogActions['didDismiss'] = (): unknown => reject();
    });

    this.notifications.notify({
      header: 'Delete comment',
      content: 'Are you sure you want to delete this comment?',
      dialogActions: dialogActions,
      type: NotificationEvent.confirm
    });

    try {
      await confirmHandler;
      deleteComment(id);
    } catch (e) {
      //no-op
    }
  }

  /**
   * Places the comment in an edit state
   */
  @action
  onEdit(): void {
    set(this, 'isEditing', true);
    set(this, 'previousComment', { ...this.comment });

    run((): void => {
      schedule('afterRender', this, 'focusEditor');
    });
  }

  /**
   * Cancels component out of edit more
   */
  @action
  onCancel(): void {
    this.stopEditing();
  }

  /**
   * Publishes the updated comment upstream
   */
  @action
  updateCommentAction(): void {
    const { comment, previousComment } = this;

    // Ensure that we have a change in the comment text
    if (comment.text !== previousComment.text) {
      this.stopEditing();
      const updateComment = this.updateComment;
      updateComment(comment.id, comment);

      return;
    }

    this.notifications.notify({ type: NotificationEvent.info, content: 'Nothing seems to have been changed?' });
  }
}
