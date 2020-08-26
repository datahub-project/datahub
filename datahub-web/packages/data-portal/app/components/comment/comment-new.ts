import Component from '@ember/component';
import { setProperties, set } from '@ember/object';
import { run, schedule, scheduleOnce } from '@ember/runloop';
import { CommentTypeUnion, IDatasetComment } from 'datahub-web/typings/api/datasets/comments';
import { baseCommentEditorOptions, newCommentEditorOptions } from 'datahub-web/constants';
import { noop } from 'lodash';
import { action } from '@ember/object';
import { classNames, tagName, className } from '@ember-decorators/component';
import { alias } from '@ember/object/computed';

/**
 * Returns initial properties for a new Comment
 * @return {Partial<IDatasetComment>}
 */
const instantiateComment = (): Partial<IDatasetComment> => ({ type: 'Comment', text: '' });
@tagName('li')
@classNames('comment-item', 'comment-item--editable', 'nacho-container')
export default class CommentNew extends Component {
  /**
   * Flag indicating the state of the component
   * @type {boolean}
   */
  @className('comment-new')
  isEditing = false;

  /**
   * The currently set value for the type property on the Comment
   * @type {string}
   */
  @alias('comment.type')
  commentType: IDatasetComment['type'];

  /**
   * A partial representation of a dataset comment
   * @type {Partial<IDatasetComment>}
   */
  comment: Partial<IDatasetComment> = {};

  createComment = noop;

  get editorOptions(): typeof baseCommentEditorOptions & typeof newCommentEditorOptions {
    return { ...baseCommentEditorOptions, ...newCommentEditorOptions };
  }

  init(): void {
    super.init();

    const comment: Partial<IDatasetComment> = instantiateComment();
    set(this, 'comment', comment);
  }

  /**
   * Applies the properties from a new instance of IDatasetComment to the comment property on the component
   */
  resetComment(): void {
    setProperties(this.comment, instantiateComment());
  }

  /**
   * Handles the click event on the component
   * and allows queue to be flushed before focusing for edits
   */
  click(): void {
    if (!this.isEditing) {
      run((): void => {
        schedule('actions', this, 'toggleEdit');
        scheduleOnce('afterRender', this, 'focusEditor');
      });
    }
  }

  /**
   * Toggle the edit flag
   */
  toggleEdit(): void {
    this.toggleProperty('isEditing');
  }

  /**
   * Focuses on the comment text editor
   */
  focusEditor(): void {
    const commentContent = this.element.querySelector<HTMLElement>('.comment-new__content');

    commentContent && commentContent.focus();
  }

  /**
   * Passes the comment text and type to the upstream handler
   * @return {Promise<void>}
   */
  @action
  async publishComment(): Promise<void> {
    const createComment = this.createComment;
    const { type, text } = this.comment;

    await createComment({ type, text });
    this.resetComment();
  }

  /**
   * Updates the type attribute on the comment property
   * @param {CommentTypeUnion} type
   */
  @action
  changeCommentType(type: CommentTypeUnion): void {
    const { comment } = this;
    set(comment, 'type', type);
  }
}
