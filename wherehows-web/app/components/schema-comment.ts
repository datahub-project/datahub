import Component from '@ember/component';
import ComputedProperty from '@ember/object/computed';
import MutableArray from '@ember/array/mutable';
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
  comments: MutableArray<IDatasetComment>;
}

export class SchemaComment extends Component {
  comments = [];
  count = 0;
  datasetId = 0;
  columnId = 0;
  isShowingFieldComment = false;

  /**
   * 
   * 
   * @memberof SchemaComment
   */
  notifications: ComputedProperty<Notifications> = inject();

  SchemaCommentActions = SchemaCommentActions;

  getComments = task(function*({ datasetId, columnId, comments }: IGetCommentsTaskArgs) {
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

      return get(this, 'getComments').perform(props);
    },

    hideComments(this: SchemaComment) {
      return set(this, 'isShowingFieldComment', false);
    },

    async handleSchemaComment(this: SchemaComment, strategy: keyof typeof SchemaCommentActions) {
      const [, { text }] = arguments;
      const { datasetId, columnId, notifications, comments, getComments } = getProperties(this, [
        'datasetId',
        'columnId',
        'notifications',
        'comments',
        'getComments'
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
        getComments.perform({ datasetId, columnId, comments });
      } catch (e) {
        notify(NotificationEvent.error, { content: e.message });
      }

      return false;
    }
  };
}
