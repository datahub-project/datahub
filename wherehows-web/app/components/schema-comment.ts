import Component from '@ember/component';
import ComputedProperty from '@ember/object/computed';
import { getProperties, get, set } from '@ember/object';
import { inject } from '@ember/service';
import { assert } from '@ember/debug';
import { task } from 'ember-concurrency';
import {
  readDatasetSchemaComments,
  createDatasetSchemaComment,
  updateDatasetSchemaComment,
  deleteDatasetSchemaComment
} from 'wherehows-web/utils/api/datasets/schema-comments';
import { augmentObjectsWithHtmlComments } from 'wherehows-web/utils/api/datasets/columns';
import { IDatasetComment } from 'wherehows-web/typings/api/datasets/comments';
import { IDatasetColumn } from 'wherehows-web/typings/api/datasets/columns';
import Notifications, { NotificationEvent } from 'wherehows-web/services/notifications';

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
  notifications: ComputedProperty<Notifications> = inject();

  /**
   * Enum of applicable values for schema comment actions
   * @type {SchemaCommentActions}
   */
  SchemaCommentActions = SchemaCommentActions;

  /**
   * Task to get related schema comments
   * TODO: refactor move to container component
   * @type {"ember-concurrency".Task<Promise<Array<IDatasetComment>>, (a?: IGetCommentsTaskArgs) => "ember-concurrency".TaskInstance<Promise<Array<IDatasetComment>>>>}
   */
  getCommentsTask = task(function*({ datasetId, columnId, comments }: IGetCommentsTaskArgs) {
    const schemaComments: Array<IDatasetComment> = yield readDatasetSchemaComments(datasetId, columnId);
    const withHtmlComments = augmentObjectsWithHtmlComments(
      schemaComments.map(({ text }) => <IDatasetColumn>{ comment: text })
    );
    comments.setObjects.call(comments, withHtmlComments);
  }).drop();

  actions = {
    showComments(this: SchemaComment) {
      const props = getProperties(this, ['datasetId', 'columnId', 'comments']);
      set(this, 'isShowingFieldComment', true);

      // @ts-ignore ts limitation with the ember object model, fixed in ember 3.1 with es5 getters
      return get(this, 'getComments').perform(props);
    },

    hideComments(this: SchemaComment) {
      return set(this, 'isShowingFieldComment', false);
    },

    /**
     * Given a schema comment action, invokes the related action to process the schema comment
     * @param {SchemaCommentActions} strategy
     * @return {Promise<boolean>}
     */
    async handleSchemaComment(this: SchemaComment, strategy: SchemaCommentActions) {
      const [, { text }] = arguments;
      const { datasetId, columnId, notifications, comments, getCommentsTask } = getProperties(this, [
        'datasetId',
        'columnId',
        'notifications',
        'comments',
        'getCommentsTask'
      ]);
      const { notify } = notifications;

      assert(`Expected action to be one of ${Object.keys(SchemaCommentActions)}`, strategy in SchemaCommentActions);

      const action = {
        add: (): Promise<void> => createDatasetSchemaComment(datasetId, columnId, text),
        modify: (): Promise<void> => updateDatasetSchemaComment(datasetId, columnId, text),
        destroy: (): Promise<void> => deleteDatasetSchemaComment(datasetId, columnId)
      }[strategy];

      try {
        await action();
        notify(NotificationEvent.success, { content: 'Success!' });
        // @ts-ignore ts limitation with the ember object model, fixed in ember 3.1 with es5 getters
        getCommentsTask.perform({ datasetId, columnId, comments });
      } catch (e) {
        notify(NotificationEvent.error, { content: e.message });
      }

      return false;
    }
  };
}
