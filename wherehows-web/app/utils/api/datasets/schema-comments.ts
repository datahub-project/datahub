import { IDatasetComment, IDatasetCommentsGetResponse } from 'wherehows-web/typings/api/datasets/comments';
import { datasetColumnUrlById } from 'wherehows-web/utils/api/datasets/columns';
import { ApiStatus } from 'wherehows-web/utils/api/shared';
import { getJSON, postJSON, deleteJSON, putJSON } from 'wherehows-web/utils/api/fetcher';
import { IDatasetSchemaCommentResponse } from 'wherehows-web/typings/api/datasets/schema-comments';

/**
 *
 * @param {number} datasetId
 * @param {number} columnId
 */
const datasetSchemaCommentUrlById = (datasetId: number, columnId: number) =>
  `${datasetColumnUrlById(datasetId)}/${columnId}/comments`;

/**
 *
 * @param {number} datasetId
 * @param {number} columnId
 * @return {Promise<Array<IDatasetComment>>}
 */
const readDatasetSchemaComments = async (datasetId: number, columnId: number): Promise<Array<IDatasetComment>> => {
  const defaultData: { comments: Array<IDatasetComment> } = { comments: [] };
  const { status, data: { comments = [] } = defaultData } = await getJSON<IDatasetCommentsGetResponse>({
    url: datasetSchemaCommentUrlById(datasetId, columnId)
  });

  if (status === ApiStatus.OK) {
    return comments;
  }

  throw new Error('');
};

const createDatasetSchemaComment = async (datasetId: number, columnId: number, comment: string) => {
  const { status } = await postJSON<IDatasetSchemaCommentResponse>({
    url: datasetSchemaCommentUrlById(datasetId, columnId),
    data: { comment }
  });

  if (status !== ApiStatus.OK) {
    throw new Error();
  }
};

const deleteDatasetSchemaComment = async (datasetId: number, columnId: number): Promise<void> => {
  const { status } = await deleteJSON<IDatasetSchemaCommentResponse>({
    url: datasetSchemaCommentUrlById(datasetId, columnId)
  });

  if (status !== ApiStatus.OK) {
    throw new Error();
  }
};

const updateDatasetSchemaComment = async (datasetId: number, columnId: number, comment: string): Promise<void> => {
  const { status } = await putJSON<IDatasetSchemaCommentResponse>({
    url: datasetSchemaCommentUrlById(datasetId, columnId),
    data: { comment }
  });

  if (status !== ApiStatus.OK) {
    throw new Error();
  }
};

export {
  readDatasetSchemaComments,
  createDatasetSchemaComment,
  deleteDatasetSchemaComment,
  updateDatasetSchemaComment
};
