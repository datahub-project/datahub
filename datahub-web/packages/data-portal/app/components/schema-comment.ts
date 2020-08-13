import Component from '@ember/component';
import { set } from '@ember/object';
import {
  readDatasetSchemaComments,
  createDatasetSchemaComment,
  updateDatasetSchemaComment,
  deleteDatasetSchemaComment
} from 'datahub-web/utils/api/datasets/schema-comments';
import { augmentObjectsWithHtmlComments } from 'datahub-web/utils/api/datasets/columns';
import { IDatasetComment } from 'datahub-web/typings/api/datasets/comments';
import { IDatasetColumn } from 'datahub-web/typings/api/datasets/columns';
import Notifications from '@datahub/utils/services/notifications';
import { NotificationEvent } from '@datahub/utils/constants/notifications';
import { action } from '@ember/object';
import { inject as service } from '@ember/service';
import { TaskInstance, task } from 'ember-concurrency';
import { ETask } from '@datahub/utils/types/concurrency';

enum SchemaCommentActions {
  modify = 'modify',
  destroy = 'destroy',
  add = 'add'
}

interface IGetCommentsTaskArgs {
  datasetId: number;
  columnId: number;
  comments: Array<IDatasetComment>;
}

export class SchemaComment extends Component {
  comments: Array<IDatasetComment> = [];
  count = 0;
  datasetId = 0;
  columnId = 0;
  isShowingFieldComment = false;

  /**
   * Local reference to the notifications service
   * @memberof SchemaComment
   */
  @service
  notifications: Notifications;

  /**
   * Enum of applicable values for schema comment actions
   * @type {SchemaCommentActions}
   */
  SchemaCommentActions = SchemaCommentActions;

  /**
   * Task to get related schema comments
   * TODO: refactor move to container component
   */
  @task(function*({
    datasetId,
    columnId,
    comments
  }: IGetCommentsTaskArgs): IterableIterator<Promise<Array<IDatasetComment>>> {
    const schemaComments = ((yield readDatasetSchemaComments(datasetId, columnId)) as unknown) as Array<
      IDatasetComment
    >;

    if (Array.isArray(schemaComments)) {
      const withHtmlComments = augmentObjectsWithHtmlComments(
        schemaComments.map(
          ({ text }): Partial<IDatasetColumn> => ({
            comment: text
          })
        ) as Array<IDatasetColumn>
      );
      comments.setObjects.call(comments, withHtmlComments);
    }
  })
  getCommentsTask!: ETask<Promise<Array<IDatasetComment>>, IGetCommentsTaskArgs>;

  @action
  showComments(): TaskInstance<Promise<Array<IDatasetComment>>> {
    const { datasetId, columnId, comments } = this;
    set(this, 'isShowingFieldComment', true);

    return this.getCommentsTask.perform({ datasetId, columnId, comments });
  }

  @action
  hideComments(): boolean {
    return set(this, 'isShowingFieldComment', false);
  }

  /**
   * Given a schema comment action, invokes the related action to process the schema comment
   * @param {SchemaCommentActions} strategy
   * @return {Promise<boolean>}
   */
  @action
  async handleSchemaComment(strategy: SchemaCommentActions, options: { text?: string } = {}): Promise<boolean> {
    const { text = '' } = options;
    const { datasetId, columnId, notifications, comments, getCommentsTask } = this;
    const { notify } = notifications;

    const action = {
      add: (): Promise<void> => createDatasetSchemaComment(datasetId, columnId, text),
      modify: (): Promise<void> => updateDatasetSchemaComment(datasetId, columnId, text),
      destroy: (): Promise<void> => deleteDatasetSchemaComment(datasetId, columnId)
    }[strategy];

    try {
      await action();
      notify({ type: NotificationEvent.success, content: 'Success!' });
      // @ts-ignore ts limitation with the ember object model, fixed in ember 3.1 with es5 getters
      getCommentsTask.perform({ datasetId, columnId, comments });
    } catch (e) {
      notify({ type: NotificationEvent.error, content: e.message });
    }

    return false;
  }
}
