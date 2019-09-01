import Component from '@ember/component';
import { set, setProperties } from '@ember/object';
import { warn } from '@ember/debug';
import { action } from '@ember/object';
import { IDatasetColumn, IDatasetColumnWithHtmlComments } from 'wherehows-web/typings/api/datasets/columns';
import $ from 'jquery';

/**
 * Cached module reference to the class name for visually hiding toggled off schema view
 * @type {string}
 */
const hiddenClassName = 'hidden';

export default class DatasetSchema extends Component {
  /**
   * Flag indicating that the schema should be rendered in a tabular format
   * @type {boolean}
   * @default true
   */
  isTable = true;

  /**
   * Default json schema string
   * @type {string}
   */
  json = '{}';

  /**
   * List of schema properties for the dataset
   * @type {IDatasetColumnWithHtmlComments | IDatasetColumn}
   */
  schemas: Array<IDatasetColumnWithHtmlComments | IDatasetColumn> = [];

  /**
   * Reference to the jsonViewer dom element
   * @type {Element | null}
   */
  jsonViewer: Element | null = null;

  /**
   * Reference to the jsonTable dom element
   * @type {Element | null}
   */
  jsonTable: Element | null = null;

  /**
   * Constructs a readable JSON structure of the dataset schema
   * @param {Element} jsonViewer
   * @return {JQuery}
   */
  buildJsonView(this: DatasetSchema, jsonViewer: Element): JQuery<Element> | void {
    try {
      return $(jsonViewer).JSONView(JSON.parse(this.json));
    } catch (e) {
      warn(`Exception occurred parsing JSON schema: ${e.message}`, false, {
        id: 'dataset-schema-parse-error'
      });
    }
  }

  /**
   * Constructs a table view of the dataset schema
   * @param {Element} jsonTable
   * @return {JQuery}
   */
  buildTableView = (jsonTable: Element) => $(jsonTable).treegrid();

  /**
   * Builds or rebuild the dataset schema / json view based on a flag to toggle this behaviour
   */
  buildView(this: DatasetSchema): void {
    const { jsonTable, jsonViewer, isTable } = this;
    if (isTable) {
      jsonTable && this.buildTableView(jsonTable);
    } else {
      jsonViewer && this.buildJsonView(jsonViewer);
    }
  }

  /**
   * Retains references to the DOM elements for showing the schema
   */
  cacheDomReference(this: DatasetSchema): void {
    const { jsonViewer, jsonTable, element } = this;

    if (!(jsonViewer && jsonTable)) {
      setProperties(this, {
        jsonViewer: element.querySelector('#json-viewer'),
        jsonTable: element.querySelector('#json-table')
      });
    }
  }

  didReceiveAttrs(): void {
    this.buildView();
  }

  didRender(): void {
    this.cacheDomReference();
  }

  willDestroyElement(this: DatasetSchema): void {
    setProperties(this, {
      jsonViewer: null,
      jsonTable: null
    });
  }

  /**
   * Handles the toggling of table vs. json view of dataset schema information
   * @param {"table" | "json"} view
   */
  @action
  showView(this: DatasetSchema, view: 'table' | 'json' = 'table'): void {
    const isTable = set(this, 'isTable', view === 'table');
    const { jsonViewer, jsonTable } = this;

    if (jsonTable && jsonViewer) {
      this.buildView();

      isTable
        ? (jsonViewer.classList.add(hiddenClassName), jsonTable.classList.remove(hiddenClassName))
        : (jsonViewer.classList.remove(hiddenClassName), jsonTable.classList.add(hiddenClassName));
    }
  }
}
