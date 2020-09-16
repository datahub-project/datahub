import { IGraphViewerState } from '@datahub/shared/types/graph/graph-viewer-state';
import { action, setProperties } from '@ember/object';
import { readLineage } from '@datahub/shared/api/lineage/index';
import EmberConcurrencyTaskContainer from '@datahub/shared/utils/ember-concurrency-task-container';
import { DataModelEntityInstance } from '@datahub/data-models/constants/entity';
import { extractEntityType } from '@datahub/utils/validators/urn';
import DataModelsService from '@datahub/data-models/services/data-models';
import { inject as service } from '@ember/service';
import { IPropertiesPanelArgs } from '../entity/properties-panel';
import titleize from 'ember-cli-string-helpers/utils/titleize';

interface ILineageLineageContainerArgs {
  // urn of the entity that you need to fetch the lineage graph
  urn: string;

  // if the graph should render full screen
  fullscreenMode: boolean;
}
/**
 * Default lineage depth for graphs
 */
const defaultLineageDepth = 3;

/**
 * Lineage Graph Container
 *
 * It will fetch lineage graph model given an entity urn.
 *
 * After loading the model, it will create the graph state and manage changes.
 */
export default class LineageLineageContainer<T = {}> extends EmberConcurrencyTaskContainer<
  ILineageLineageContainerArgs & T
> {
  /**
   * Current State of Graph Viewer
   */
  state?: IGraphViewerState = {
    lineageMode: true,
    lineageDepth: defaultLineageDepth,
    showAttributes: true,
    fullscreenMode: this.args.fullscreenMode
  };

  /**
   * Last State of Graph Viewer useful for detecting changes with current state
   */
  lastState?: IGraphViewerState;

  /**
   * Data Models Service will get the model given the APIName in the urn and then create a partial instance
   */
  @service
  dataModels!: DataModelsService;

  /**
   * Creates a data model instance given the urn passed in the arguments
   *
   * @readonly
   */
  get entity(): DataModelEntityInstance | undefined {
    const entityUrn = this.args.urn;
    const apiEntityType = extractEntityType(entityUrn);
    const EntityModel = apiEntityType && this.dataModels?.getModelByApiName(apiEntityType);
    if (EntityModel && entityUrn) {
      return this.dataModels?.createPartialInstance(EntityModel?.displayName, entityUrn);
    }
    return;
  }

  /**
   * For full screen mode, ui will use properties panel to render the entity link name and link
   *
   * @readonly
   */
  get propertiesPanelOptions(): IPropertiesPanelArgs['options'] {
    return {
      columnNumber: 1,
      standalone: false,
      properties: [
        {
          name: 'entityLink.link',
          displayName: titleize(this.entity?.displayName),
          component: {
            name: 'link/optional-value',
            options: {
              openNewTab: true
            }
          }
        }
      ]
    };
  }

  /**
   * Will fetch lineage graph information and set it in the initial state along with
   * initial values
   */
  async containerInit(): Promise<void> {
    const graphModel = await readLineage(this.args.urn, this.state?.lineageDepth || defaultLineageDepth);
    setProperties(this, {
      state: {
        ...this.state,
        graph: graphModel
      },
      lastState: undefined
    });
  }

  /**
   * State change handle will set the new state and save the last one
   * @param newState
   */
  @action
  onStateChanged(newState: IGraphViewerState): void {
    const lastState = this.state;
    const state = newState;

    setProperties(this, {
      state,
      lastState
    });

    if (lastState?.lineageDepth !== state.lineageDepth) {
      this.concurrencyTask.perform();
    }
  }
}
