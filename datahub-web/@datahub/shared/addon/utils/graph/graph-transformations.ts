import { keyBy, groupBy } from 'lodash';
import minimatch from 'minimatch';

type NodeId = Com.Linkedin.Metadata.Graph.NodeId;
type Node = Com.Linkedin.Metadata.Graph.Node;
type Graph = Com.Linkedin.Metadata.Graph.Graph;
type Edge = Com.Linkedin.Metadata.Graph.Edge;

/**
 * Will convert old style reference to a new style
 * where the edges are stored in a separated array (insted of inside the node)
 * @param graph
 */
export function moveAttributeEdgesToEdges(graph: Graph): Graph {
  const edges = graph.nodes.reduce((edges: Array<Edge>, node: Node): Array<Edge> => {
    const attributesWithReferences = node.attributes?.filter(attribute => attribute.reference);
    const newEdges = attributesWithReferences?.map(
      (attribute): Edge => {
        return {
          fromNode: node.id,
          fromAttribute: attribute.name,
          toNode: attribute.reference || '',
          attributes: [
            {
              name: node.displayName || node.id,
              value: attribute.name
            }
          ]
        };
      }
    );
    return [...edges, ...(newEdges || [])];
  }, []);

  return { ...graph, edges: [...(graph.edges || []), ...edges] };
}

/**
 * Will remove attributes that does not contain a reference from all nodes
 * @param graph
 */
export function excludeSimpleAttributes(graph: Graph): Graph {
  return {
    ...graph,
    nodes: [
      ...graph.nodes.map(
        (node): Node => ({
          ...node,
          attributes: node.attributes && node.attributes.filter((attribute): boolean => Boolean(attribute.reference))
        })
      )
    ]
  };
}

/**
 * Interface to represent an exploration path for getAllPaths
 */
interface IPathNode {
  // Id of the current node
  nodeId: NodeId;
  // Number of hops
  score: number;
  // Nodes explored
  nodes: Array<NodeId>;
  // Edges used
  edges: Array<Edge>;
  // Nodes explored as a hashmap to resolve quick checks
  visited: Record<NodeId, boolean>;
}

/**
 * Return type of fn getAllPaths
 */
interface IPathResult {
  /**
   * Nodes explored for a path
   */
  nodes: Array<NodeId>;
  /**
   * Edges used for a path
   */
  edges: Array<Edge>;
}

/**
 * Will return all Paths available between two nodes
 * @param graph Graph with nodes and edges
 * @param from node id from the origin of the path
 * @param to destination node id of the path
 */
export function getAllPaths(graph: Graph, from: NodeId, to: NodeId): Array<IPathResult> {
  const result: Array<IPathResult> = [];
  // Start on initial node
  let nextNodes: Array<IPathNode> = [{ nodeId: from, score: 0, nodes: [from], edges: [], visited: {} }];

  if (from === to) {
    return [];
  }

  // while there is nodes to explore
  while (nextNodes.length > 0) {
    nextNodes = nextNodes.reduce((nextNodes, currentNode) => {
      // Pick all edges that start on currentNode and does not end on a visited node
      const edges = graph.edges?.filter(
        edge => edge.fromNode === currentNode.nodeId && !currentNode.visited[edge.toNode]
      );

      return [
        ...nextNodes,
        // Add new exploration paths following the new edges
        ...(edges?.map(edge => {
          const nodes = [...currentNode.nodes, edge.toNode];
          const edges = [...currentNode.edges, edge];

          // If we reach the desired node, we save the result but keep exploring as we
          // need all possible paths
          if (edge.toNode === to) {
            result.push({
              nodes,
              edges
            });
          }

          // New exploration path with edge destination node, marking current node as visited for this exploration path
          return {
            nodeId: edge.toNode,
            score: currentNode.score + 1,
            nodes,
            edges,
            visited: { ...currentNode.visited, [currentNode.nodeId]: true }
          };
        }) || [])
      ];
    }, []);
  }
  return result || [];
}

/**
 * Will remove the islands that are not connected to the rootNode
 * @param graph
 */
export function removeIslands(graph: Graph): Graph {
  if (graph.rootNode) {
    const nodesIndex: Record<Node['id'], Node> = keyBy(graph.nodes, 'id');
    const edges = (graph.edges || []).filter(edge => nodesIndex[edge.fromNode] && nodesIndex[edge.toNode]);
    const fromIndex: Record<Node['id'], Array<Edge>> = groupBy(graph.edges, 'fromNode');
    const toIndex: Record<Node['id'], Array<Edge>> = groupBy(graph.edges, 'toNode');

    const nodes: Array<Node> = [];
    const visited: Record<Node['id'], boolean> = {};
    let exploring: Array<Node['id']> = [graph.rootNode];

    while (exploring.length > 0) {
      exploring = exploring.reduce((exploring, nodeId) => {
        if (!visited[nodeId] && nodesIndex[nodeId]) {
          visited[nodeId] = true;
          nodes.push(nodesIndex[nodeId]);
          return [
            ...exploring,
            ...(fromIndex[nodeId] || []).map(edge => edge.toNode),
            ...(toIndex[nodeId] || []).map(edge => edge.fromNode)
          ];
        }
        return exploring;
      }, []);
    }

    const cleanNodesIndex: Record<Node['id'], Node> = keyBy(nodes, 'id');
    const cleanEdges = edges.filter(edge => cleanNodesIndex[edge.fromNode] && cleanNodesIndex[edge.toNode]);

    return { ...graph, nodes, edges: cleanEdges };
  }
  return graph;
}

/**
 * Exclude nodes from a graph give a glob pattern
 * @param graph
 * @param globPattern
 */
export function excludeNodes(graph: Graph, globPattern: string | undefined): Graph {
  if (globPattern) {
    const { nodes } = graph;
    const patterns = globPattern.split('\n');
    const included = nodes.filter(node =>
      patterns.every(pattern => !minimatch(node.displayName || node.id, pattern, { matchBase: true }))
    );
    const withoutIslands = removeIslands({
      ...graph,
      nodes: included
    });
    return withoutIslands;
  }
  return graph;
}
