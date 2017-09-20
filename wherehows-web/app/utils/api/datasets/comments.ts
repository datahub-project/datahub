import Ember from 'ember';
import { IDatasetComment, IDatasetCommentsGetResponse } from 'wherehows-web/typings/api/datasets/comments';
import { datasetUrlById } from 'wherehows-web/utils/api/datasets/shared';
import { ApiStatus } from 'wherehows-web/utils/api/shared';

const { $: { getJSON, post, ajax } } = Ember;

// TODO:  DSS-6122 Create and move to Error module
/**
 * default message for comment api exception
 * @type {string}
 */
const datasetCommentsApiException = 'An error occurred with the comments api';

/**
 *
 * @type {string}
 */
const csrfToken = '_UNUSED_';

/**
 * Returns a dataset comment url for a given Id
 * @param {number} id the dataset Id
 * @return {string}
 */
const datasetCommentsUrlById = (id: number): string => `${datasetUrlById(id)}/comments`;

/**
 * Gets a specific comment on a dataset
 * @param {number} datasetId the id of the dataset
 * @param {number} commentId the id of the comment
 * @return {string}
 */
const datasetCommentUrlById = (datasetId: number, commentId: number): string =>
  `${datasetCommentsUrlById(datasetId)}/${commentId}`;

/**
 * For a given dataset Id, fetches the list of comments associated with the dataset
 * @param {number} id the id of the dataset
 * @return {Promise<Array<IDatasetComment>>}
 */
const readDatasetComments = async (id: number): Promise<Array<IDatasetComment>> => {
  const response: IDatasetCommentsGetResponse = await Promise.resolve(getJSON(datasetCommentsUrlById(id)));
  const { status, data: { comments } } = response;

  if (status === ApiStatus.OK) {
    return comments;
  }

  throw new Error(datasetCommentsApiException);
};

/**
 * Posts a new comment on a dataset
 * @param {number} id the id of the dataset
 * @param {CommentTypeUnion} type the comment type
 * @param {string} text the comment
 * @return {Promise<void>}
 */
const createDatasetComment = async (
  id: number,
  { type, text }: Pick<IDatasetComment, 'type' | 'text'>
): Promise<void> => {
  const response: { status: ApiStatus } = await Promise.resolve(
    post({
      url: datasetCommentsUrlById(id),
      headers: {
        'Csrf-Token': csrfToken
      },
      data: {
        type,
        text
      }
    })
  );

  const { status } = response;

  // Comments api uses the ApiStatus.SUCCESS string literal
  if (status !== ApiStatus.SUCCESS) {
    throw new Error(datasetCommentsApiException);
  }
};

/**
 * Deletes a dataset comment using the mid-tier endpoint
 * @param {number} datasetId the id of the dataset that has the comment
 * @param {number} commentId the id of the comment on the specified dataset
 * @return {Promise<void>}
 */
const deleteDatasetComment = async (datasetId: number, commentId: number): Promise<void> => {
  const response: { status: ApiStatus } = await Promise.resolve(
    ajax({
      url: datasetCommentUrlById(datasetId, commentId),
      method: 'DELETE',
      headers: {
        'Csrf-Token': csrfToken
      },
      dataType: 'json'
    })
  );

  const { status } = response;

  if (status !== ApiStatus.SUCCESS) {
    throw new Error(datasetCommentsApiException);
  }
};

/**
 * Modifies a previous comment on this dataset
 * @param {number} datasetId the dataset with the comment to be modified
 * @param {number} commentId the comment to be modified
 * @param {CommentTypeUnion} type the type of the comment
 * @param {string} text the updated comment text
 * @return {Promise<void>}
 */
const updateDatasetComment = async (
  datasetId: number,
  commentId: number,
  { type, text }: IDatasetComment
): Promise<void> => {
  const response: { status: ApiStatus } = await Promise.resolve(
    ajax({
      url: datasetCommentUrlById(datasetId, commentId),
      method: 'PUT',
      headers: {
        'Csrf-Token': csrfToken
      },
      dataType: 'json',
      data: { type, text }
    })
  );

  const { status } = response;
  if (status !== ApiStatus.SUCCESS) {
    throw new Error(datasetCommentsApiException);
  }
};

export { readDatasetComments, createDatasetComment, deleteDatasetComment, updateDatasetComment };
