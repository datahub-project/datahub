import Ember from 'ember';
import { CommentTypeUnion, IDatasetComment } from 'wherehows-web/typings/api/datasets/comments';
import { baseCommentEditorOptions, newCommentEditorOptions } from 'wherehows-web/constants';

const { Component, set, get, setProperties, computed, assert, run } = Ember;
const { schedule, scheduleOnce } = run;

/**
 * Returns initial properties for a new Comment
 * @return {Partial<IDatasetComment>}
 */
const instantiateComment = (): Partial<IDatasetComment> => ({ type: 'Comment', text: '' });

const CommentNew = Component.extend({
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

  didReceiveAttrs() {
    // Assert that the createComment prop is a function
    const typeOfCreateComment = typeof this.attrs.createComment;
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
      set(this, 'comment.type', type);
    }
  }
});

Ember.Object.reopen.call(CommentNew, {
  init() {
    this._super(...Array.from(arguments));

    const comment: Partial<IDatasetComment> = instantiateComment();
    set(this, 'comment', comment);
  }
});

export default CommentNew;
