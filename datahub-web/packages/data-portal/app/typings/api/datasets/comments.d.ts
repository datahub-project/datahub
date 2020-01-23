import { ApiStatus } from '@datahub/utils/api/shared';

// Allowable values for the Comment type property
export type CommentTypeUnion =
  | 'Comment'
  | 'Question'
  | 'Description'
  | 'Partition'
  | 'ETL Schedule'
  | 'Grain'
  | 'DQ Issue';

/**
 * Describes the interface for a Dataset Comment
 */
export interface IDatasetComment {
  type: CommentTypeUnion;
  text: string;
  isAuthor: boolean;
  authorName: string;
  authorEmail: string;
  authorUserName: string;
  created: string; // should be in epoch ms
  modified: string; // should be in epoch ms
  datasetId: number;
  id: number;
}

/**
 * Describes the response from the comment api get request
 */
export interface IDatasetCommentsGetResponse {
  status: ApiStatus;
  data: {
    count: number;
    itemsPerPage: number;
    totalPages: number;
    comments: Array<IDatasetComment>;
  };
}
