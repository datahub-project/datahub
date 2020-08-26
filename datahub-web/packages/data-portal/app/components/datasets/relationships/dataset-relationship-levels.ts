import Component from '@ember/component';
import { classNames } from '@ember-decorators/component';
import { computed } from '@ember/object';
import GraphDb, { INode, IEdge } from 'datahub-web/utils/graph-db';
import { IDatasetLineage } from '@datahub/metadata-types/types/entity/dataset/lineage';
import { IPropertiesPanelArgs } from '@datahub/shared/components/entity/properties-panel';

/**
 * Interface that adds more fields to a standard node
 */
interface IExpandedNode<T> extends INode<T> {
  // number of children of this node
  nChildren: string;
  // number of parents of this node
  nParents: string;
}

/**
 * Interface to define the properties of a level that are
 * useful to render a level on the ui
 */
interface ILevel {
  nodes: Array<IExpandedNode<IDatasetLineage>>;
  collapsed: boolean;
  show: boolean;
  level: number;
  label: string;
}

/**
 * returns the title of a level given the level number
 * @param level number of the level
 */
const getLabel = (level: number): string => {
  if (level < 0) {
    return `Up ${Math.abs(level)} Level`;
  }

  if (level > 0) {
    return `Down ${Math.abs(level)} Level`;
  }

  if (level === 0) {
    return `Current Dataset`;
  }

  return '';
};

/**
 * This component will render a list of nodes grouped by levels, leveraging search-result view
 */
@classNames('dataset-relationship-levels')
export default class DatasetRelationshipLevels extends Component {
  /**
   * Nodes to render on this page
   */
  nodes!: Array<INode<IDatasetLineage>>;

  /**
   * Relationships between nodes
   */
  edges!: Array<IEdge<IDatasetLineage>>;

  /**
   * Action passed into this component. Will be trigger when a node is selected
   * @param _node
   */
  toggleNode!: (node: INode<IDatasetLineage>) => void;

  /**
   * Fields to render for the dataset. We are going to transform the default
   * ones and correct the path given that the dataset is wrapped inside a node.
   * Also we are going to add a couple of more properties that belongs to the node
   * and lineage relation.
   */
  propertiesPanelOptions: IPropertiesPanelArgs['options'] = {
    standalone: false,
    columnNumber: 2,
    properties: [
      {
        name: 'payload.dataset.fabric',
        displayName: 'Data Origin'
      },
      {
        name: 'payload.dataset.platform',
        displayName: 'Platform'
      },
      {
        name: 'payload.type',
        displayName: 'Type'
      },
      {
        name: 'payload.actor',
        displayName: 'Actor'
      },
      {
        name: 'nChildren',
        displayName: '# Children'
      },
      {
        name: 'nParents',
        displayName: '# Parents'
      }
    ]
  };

  /**
   * We are going to transform nodes and edges into a graphDb for
   * convenience, that way it is easier to query the number of children or parents.
   */
  @computed('nodes', 'edges')
  get graphDb(): GraphDb<IDatasetLineage> {
    return GraphDb.create({
      nodes: this.nodes,
      edges: this.edges
    }) as GraphDb<IDatasetLineage>;
  }

  /**
   * Will check if we should show a node. In order to determine that, we need to check if the node
   * is upstream or downstream. If the node is downstream, them node will show if parent is selected,
   * if upstream, node will show is child is selected.
   * Also, root node is always visible.
   */
  getShouldShow(node: INode<IDatasetLineage>): boolean {
    const children = this.graphDb.childrenByNodeId[node.id];
    const parents = this.graphDb.parentsByNodeId[node.id];
    const parentSelected = parents.any((parent): boolean => !!parent.selected);
    const childSelected = children.any((child): boolean => !!child.selected);
    const isRootNode = node.level === 0;
    const isDownStreamNodeWithParentSelected = parentSelected && node.level > 0;
    const isUpStreamNodeWithNodeSelected = childSelected && node.level < 0;
    return isRootNode || isDownStreamNodeWithParentSelected || isUpStreamNodeWithNodeSelected;
  }

  /**
   * Will generate a level given the first node of the level
   * @param node
   */
  generateLevel(node: INode<IDatasetLineage>): ILevel {
    const level: ILevel = {
      label: getLabel(node.level),
      level: node.level,
      collapsed: false,
      show: false,
      nodes: []
    };
    return level;
  }

  /**
   * Will add a node to a level. A level is shown if any of the nodes is visible. A level is collapse
   * if any of its nodes is selected
   * @param level
   * @param node
   */
  addNodeToLevel(level: ILevel, node: IExpandedNode<IDatasetLineage>): ILevel {
    const shouldShow = this.getShouldShow(node);
    if (shouldShow) {
      return {
        ...level,
        show: level.show || shouldShow,
        collapsed: level.collapsed || !!node.selected,
        nodes: [...level.nodes, node]
      };
    }
    return level;
  }

  /**
   * Will add more properties to the regular nodes like the number of parents of children
   */
  @computed('graphDb.nodes')
  get decoratedNodes(): Array<IExpandedNode<IDatasetLineage>> {
    return this.graphDb.nodes.map(
      (node): IExpandedNode<IDatasetLineage> => ({
        ...node,
        nChildren: node.loaded || node.level < 0 ? `${this.graphDb.childrenByNodeId[node.id].length}` : '-',
        nParents: node.loaded || node.level > 0 ? `${this.graphDb.parentsByNodeId[node.id].length}` : '-'
      })
    );
  }

  /**
   * Will transform the list of nodes into an array of levels and inside those levels,
   * you can find the nodes.
   */
  @computed('decoratedNodes')
  get computedNodes(): Array<ILevel> {
    return this.decoratedNodes.reduce((nodesArr: Array<ILevel>, node): Array<ILevel> => {
      const positionArray = node.level - this.graphDb.minLevel;
      const newLevel = nodesArr[positionArray] || this.generateLevel(node);
      const result = [...nodesArr];
      result[positionArray] = this.addNodeToLevel(newLevel, node);
      return result;
    }, []);
  }
}
