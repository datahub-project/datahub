import Component from '@ember/component';
import { classNames } from '@ember-decorators/component';
import vis, { Network, Node as VisNode, Options } from 'vis';
import { IDatasetLineage } from '@datahub/metadata-types/types/entity/dataset/lineage';
import { INode, IEdge } from 'datahub-web/utils/graph-db';
import { set } from '@ember/object';
import { computed, action } from '@ember/object';

/**
 * Default vis options for the chart
 */
const defaultVisOptions: Options = {
  width: '300px',
  height: '400px',
  nodes: {
    shape: 'dot',
    borderWidth: 0,
    borderWidthSelected: 0,
    color: {
      hover: '#1a84bc'
    }
  },
  layout: {
    hierarchical: {
      direction: 'LR'
    }
  },
  edges: {
    arrows: 'to'
  },
  interaction: {
    zoomView: false,
    dragNodes: false,
    dragView: true,
    hover: true
  }
};

/**
 * Will add more apis to vis network
 */
type INetworkExtended = Network & {
  canvas: {
    body: {
      container: {
        style: {
          cursor: 'pointer' | 'default';
        };
      };
    };
  };
};

/**
 * Will return the color of the node based on the state of the node.
 * @param node
 */
const getNodeColor = (node: INode<IDatasetLineage>): string => {
  if (node.selected) {
    return '#ef7e37';
  }

  if (!node.loaded) {
    return '#c7d1d8';
  }

  return '#98d8f4';
};

/**
 * Class that will make use of VIS to render a tree
 */
@classNames('dataset-relationship-vis')
export default class DatasetRelationshipVis extends Component {
  /**
   * Nodes to render
   */
  nodes: Array<INode<IDatasetLineage>> = [];
  /**
   * Edges that links the nodes
   */
  edges: Array<IEdge<IDatasetLineage>> = [];

  /**
   * Action that is passed into the component that will be triggered when a node is clicked
   * @param _node
   */
  toggleNode!: (node: INode<IDatasetLineage>) => void;

  /**
   * A reference to the graph once element is inserted
   */
  network?: vis.Network;

  /**
   * Will create the VIS network and attach the events needed
   */
  didInsertElement(): void {
    super.didInsertElement();
    const data = {
      nodes: this.visNodes,
      edges: this.visEdges
    };
    const container = this.element.querySelector<HTMLElement>('.dataset-relashipship-vis__container');

    if (!container) {
      throw new Error('Vis container not found');
    }

    const network: INetworkExtended = new vis.Network(container, data, defaultVisOptions) as INetworkExtended;
    set(this, 'network', network);

    network.on('selectNode', ({ nodes: [id] }: { nodes: Array<number> }) => {
      const selectedNode = this.nodes.find(node => node.id === id);
      if (selectedNode) {
        this.toggleNode(selectedNode);
      }
    });

    // Show a pointer when hovering a node
    network.on('hoverNode', () => {
      network.canvas.body.container.style.cursor = 'pointer';
    });

    // Hide pointer when blur
    network.on('blurNode', () => {
      network.canvas.body.container.style.cursor = 'default';
    });
  }

  /**
   * These are the nodes adapted to VIS api
   */
  @computed('nodes')
  get visNodes(): Array<VisNode> {
    return this.nodes.map(node => ({
      ...node,
      label: '',
      title: node.payload && node.payload.dataset.nativeName,
      color: {
        background: getNodeColor(node)
      }
    }));
  }

  /**
   * We have to change the arrow direction as this
   * represents the data flow instead of dependency tree
   */
  @computed('edges')
  get visEdges(): Array<IEdge<IDatasetLineage>> {
    return this.edges.map(edge => ({
      from: edge.to,
      to: edge.from
    }));
  }

  /**
   * If update attrs then update VIS
   */
  didUpdateAttrs(): void {
    super.didUpdateAttrs();
    const data = {
      nodes: this.visNodes,
      edges: this.visEdges
    };
    if (this.network) {
      const { network } = this;
      const scale = network.getScale();
      const position = network.getViewPosition();
      network.setData(data);
      network.once('stabilized', () =>
        network.moveTo({
          scale,
          position
        })
      );
    }
  }

  /**
   * Destroy VIS to avoid memory leaks
   */
  willDestroyElement(): void {
    if (this.network) {
      this.network.destroy();
    }
    super.willDestroyElement();
  }

  /**
   * Will center the graph in case it is not centered
   */
  @action
  fitGraph(): void {
    this.network && this.network.fit();
  }

  /**
   * Will zoom in into the graph
   */
  @action
  zoomIn(): void {
    const scale: number = (this.network && this.network.getScale()) || 1;
    this.network && this.network.moveTo({ scale: scale + scale * 0.2 });
  }

  /**
   * Will zoom out into the graph
   */
  @action
  zoomOut(): void {
    const scale: number = (this.network && this.network.getScale()) || 1;
    this.network && this.network.moveTo({ scale: scale - scale * 0.2 });
  }
}
