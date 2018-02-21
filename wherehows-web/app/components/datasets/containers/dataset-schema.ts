import Component from '@ember/component';
import { get, setProperties } from '@ember/object';
import { task } from 'ember-concurrency';
import { IDatasetColumn, IDatasetColumnWithHtmlComments } from 'wherehows-web/typings/api/datasets/columns';
import { IDatasetSchema } from 'wherehows-web/typings/api/datasets/schema';
import { augmentObjectsWithHtmlComments } from 'wherehows-web/utils/api/datasets/columns';
import { readDatasetSchemaByUrn } from 'wherehows-web/utils/api/datasets/schema';

export default class DatasetSchemaContainer extends Component {
  /**
   * The urn identifier for the dataset
   * @type {string}
   */
  urn: string;

  /**
   * json string for the dataset schema properties
   * @type {string}
   */
  json: string;

  /**
   * List of schema properties for the dataset
   * @type {IDatasetColumnWithHtmlComments | IDatasetColumn}
   */
  schemas: Array<IDatasetColumnWithHtmlComments | IDatasetColumn>;

  didInsertElement() {
    get(this, 'getDatasetSchemaTask').perform();
  }

  didUpdateAttrs() {
    get(this, 'getDatasetSchemaTask').perform();
  }

  /**
   * Reads the schema for the dataset
   * @type {Task<Promise<IDatasetSchema>, (a?: any) => TaskInstance<Promise<IDatasetSchema>>>}
   */
  getDatasetSchemaTask = task(function*(this: DatasetSchemaContainer): IterableIterator<Promise<IDatasetSchema>> {
    let schemas,
      { columns, rawSchema: json } = yield readDatasetSchemaByUrn(get(this, 'urn'));
    schemas = augmentObjectsWithHtmlComments(columns);
    json || (json = '{}');

    setProperties(this, { schemas, json });
  });
}
