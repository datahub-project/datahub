type Graph = Com.Linkedin.Metadata.Graph.Graph;

/**
 * State Shared by GraphViewer and toolbar
 */
export interface IGraphViewerState {
  /**
   * If user is currently searching
   */
  isSearching?: boolean;

  /**
   * Graph data
   */
  graph?: Graph;

  /**
   * User input text
   */
  typeaheadText?: string;

  /**
   * Nodes displayed to user once the user has typed something
   */
  typeaheadOptions?: Array<Com.Linkedin.Metadata.Graph.Node>;

  /**
   * Current node selected by the user
   */
  selectedNodeId?: string;

  /**
   * Show properties with references only. False by default
   */
  showAttributes?: boolean;

  /**
   * Flag to provide some enhancements useful for lineage exploration
   */
  lineageMode?: boolean;

  /**
   * Show this level of depth for this graph
   */
  lineageDepth?: number;

  /**
   * Will show full screen
   */
  fullscreenMode?: boolean;

  /**
   * String GLOB pattern used to exclude entities from the graph
   */
  excludeNodePattern?: string;

  /**
   * number of nodes that graph is showing
   */
  numberShowingNodes?: number;
}
