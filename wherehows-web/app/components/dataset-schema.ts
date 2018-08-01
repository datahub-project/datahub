import Component from '@ember/component';
import { set, get, setProperties, getProperties } from '@ember/object';
import { warn } from '@ember/debug';
import { action } from '@ember-decorators/object';
import { IDatasetColumn, IDatasetColumnWithHtmlComments } from 'wherehows-web/typings/api/datasets/columns';

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
      return $(jsonViewer).JSONView(JSON.parse(get(this, 'json')));
    } catch (e) {
      warn(`Exception occurred parsing JSON schema: ${e.message}`);
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
  buildView(this: DatasetSchema) {
    if (get(this, 'isTable')) {
      const jsonTable = get(this, 'jsonTable');
      jsonTable && this.buildTableView(jsonTable);
    } else {
      const jsonViewer = get(this, 'jsonViewer');
      jsonViewer && this.buildJsonView(jsonViewer);
    }
  }

  /**
   * Retains references to the DOM elements for showing the schema
   */
  cacheDomReference(this: DatasetSchema) {
    const { jsonViewer, jsonTable } = getProperties(this, ['jsonTable', 'jsonViewer']);

    if (!(jsonViewer && jsonTable)) {
      const dom = get(this, 'element');

      setProperties(this, {
        jsonViewer: dom.querySelector('#json-viewer'),
        jsonTable: dom.querySelector('#json-table')
      });
    }
  }

  didReceiveAttrs() {
    this.buildView();
  }

  didRender() {
    this.cacheDomReference();
  }

  willDestroyElement(this: DatasetSchema) {
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
  showView(this: DatasetSchema, view: 'table' | 'json' = 'table') {
    const isTable = set(this, 'isTable', view === 'table');
    const { jsonViewer, jsonTable } = getProperties(this, ['jsonTable', 'jsonViewer']);

    if (jsonTable && jsonViewer) {
      this.buildView();

      isTable
        ? (jsonViewer.classList.add(hiddenClassName), jsonTable.classList.remove(hiddenClassName))
        : (jsonViewer.classList.remove(hiddenClassName), jsonTable.classList.add(hiddenClassName));
    }
  }
}
