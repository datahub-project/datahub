import { IDatasetColumnWithHtmlComments } from '@datahub/datasets-core/types/datasets/columns';
import { arrayMap } from '@datahub/utils/array/index';
import { renderLinksAsAnchorTags } from '@datahub/utils/helpers/render-links-as-anchor-tags';
import { IDatasetSchemaColumn } from '@datahub/metadata-types/types/entity/dataset/schema';

// TODO:  DSS-6122 Create and move to Error module

/**
 * Maps an object with a column prop to an object containing markdown comments, if the dataset has a comment attribute
 * @template T
 * @param {T} objectWithComment
 * @returns {(T | T & {commentHtml: string})}
 */
const augmentWithHtmlComment = <T extends { comment: string }>(
  objectWithComment: T
): T | (T & { commentHtml: string }) => {
  const { comment } = objectWithComment;
  // TODO: DSS-6122 Refactor global function reference to marked
  // not using spread operator here: https://github.com/Microsoft/TypeScript/issues/10727
  // current ts version: 2.5.3
  return comment
    ? Object.assign({}, objectWithComment, { commentHtml: renderLinksAsAnchorTags([comment]) })
    : objectWithComment;
};
/**
 * Takes a list of objects with comments and returns an array of objects with comments or html comments
 * @type {(array: Array<T extends { comment: string } & Object>) => Array<T | T extends { commentHtml: string }>}
 */
export const augmentObjectsWithHtmlComments = arrayMap<
  IDatasetSchemaColumn,
  IDatasetColumnWithHtmlComments | IDatasetSchemaColumn
>(augmentWithHtmlComment);
