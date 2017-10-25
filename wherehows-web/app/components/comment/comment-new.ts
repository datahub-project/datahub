import Component from '@ember/component';
import { computed, setProperties, get, set } from '@ember/object';
import { run, schedule, scheduleOnce } from '@ember/runloop';
import { assert } from '@ember/debug';
import { CommentTypeUnion, IDatasetComment } from 'wherehows-web/typings/api/datasets/comments';
import { baseCommentEditorOptions, newCommentEditorOptions } from 'wherehows-web/constants';
import noop from 'wherehows-web/utils/noop';

/**
 * Returns initial properties for a new Comment
 * @return {Partial<IDatasetComment>}
 */
const instantiateComment = (): Partial<IDatasetComment> => ({ type: 'Comment', text: '' });

export default Component.extend({
  editorOptions: computed(() => ({ ...baseCommentEditorOptions, ...newCommentEditorOptions })).readOnly(),

  tagName: 'li',

  classNames: ['comment-item', 'comment-item--editable', 'nacho-container'],

  /**
   * Binds the isEditing flag to the comment-new class
   */
  classNameBindings: ['isEditing:comment-new'],

  /**
   * Flag indicating the state of the component
   * @type {boolean}
   */
  isEditing: false,

  /**
   * The currently set value for the type property on the Comment
   * @type {string}
   */
  commentType: computed.alias('comment.type'),

  /**
   * A partial representation of a dataset comment
   * @type {IDatasetComment | Partial<IDatasetComment>}
   */
  comment: <IDatasetComment | Partial<IDatasetComment>>{},

  createComment: noop,

  init() {
    this._super(...arguments);

    const comment: Partial<IDatasetComment> = instantiateComment();
    set(this, 'comment', comment);
  },

  didReceiveAttrs() {
    // Assert that the createComment prop is a function
    const typeOfCreateComment = typeof this.createComment;
    assert(
      `Expected action createComment to be an function (Ember action), got ${typeOfCreateComment}`,
      typeOfCreateComment === 'function'
    );
  },

  /**
   * Applies the properties from a new instance of IDatasetComment to the comment property on the component
   */
  resetComment(): void {
    setProperties(get(this, 'comment'), instantiateComment());
  },

  /**
   * Handles the click event on the component
   * and allows queue to be flushed before focusing for edits
   */
  click() {
    if (!get(this, 'isEditing')) {
      run(() => {
        schedule('actions', this, 'toggleEdit');
        scheduleOnce('afterRender', this, 'focusEditor');
      });
    }
  },

  /**
   * Toggle the edit flag
   */
  toggleEdit(): void {
    this.toggleProperty('isEditing');
  },

  /**
   * Focuses on the comment text editor
   */
  focusEditor(): void {
    this.$('.comment-new__content').focus();
  },

  actions: {
    /**
     * Passes the comment text and type to the upstream handler
     * @return {Promise<void>}
     */
    async publishComment(): Promise<void> {
      const createComment = this.createComment;
      const { type, text } = <IDatasetComment>get(this, 'comment');

      const didCreateComment = await createComment({ type, text });
      if (didCreateComment) {
        this.resetComment();
      }
    },

    /**
     * Invoked with a delay after the commenter has finished typing
     * Dispatched on a slight delay after changes are made.
     * TODO: Enhancement META-1995 - maybe use for auto-save, or other times we need to know
     * that the field contents changed
     */
    commentTextUpdated(): void {
      //TODO: META-1995 interval auto-save?
    },

    /**
     * Updates the type attribute on the comment property
     * @param {CommentTypeUnion} type
     */
    changeCommentType(type: CommentTypeUnion): void {
      const comment = get(this, 'comment');
      set(comment, 'type', type);
    }
  }
});
