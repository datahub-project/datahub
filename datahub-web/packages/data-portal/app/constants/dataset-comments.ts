import { CommentTypeUnion } from 'datahub-web/typings/api/datasets/comments';

/**
 * List of CrUD actions that can be performed on a stream comment
 */
export type StreamCommentActionsUnion = 'create' | 'update' | 'destroy';

/**
 * List of CrUD actions that can be performed on the dataset stream
 */
export type DatasetStreamActionsUnion = 'add' | 'modify' | 'destroy';

/**
 * Common options to pass to the editor component on instantiation
 * @type {object}
 */
const baseCommentEditorOptions = {
  disableDoubleReturn: true,
  disableExtraSpaces: true,
  disableEditing: false
};

/**
 * Options relevant to the new comment editor
 * @type {object}
 */
const newCommentEditorOptions = {
  placeholder: {
    text: 'Type here. You can format text by selecting it, and/or using keyboard shortcuts like cmd-b, e.t.c',
    hideOnClick: false
  }
};

/**
 * List of Dataset Comment types
 * @type {ReadonlyArray<CommentTypeUnion>}
 */
const CommentTypes: ReadonlyArray<CommentTypeUnion> = Object.freeze<CommentTypeUnion>([
  'Comment',
  'Question',
  'Description',
  'Partition',
  'ETL Schedule',
  'Grain',
  'DQ Issue'
]);

export { baseCommentEditorOptions, newCommentEditorOptions, CommentTypes };
