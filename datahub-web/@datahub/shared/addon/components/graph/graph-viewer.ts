import Component from '@glimmer/component';
import GraphRenderer from '@datahub/shared/services/graph-renderer';
import { inject as service } from '@ember/service';
import svgPanZoom from 'svg-pan-zoom';
import { processGraphVizSvg, forEachNode } from '@datahub/shared/utils/graph/graph-svg';
import { focusElements, calculateMaxZoom } from '@datahub/shared/utils/graph/svg-pan-and-zoom';
import {
  excludeSimpleAttributes,
  moveAttributeEdgesToEdges,
  getAllPaths
} from '@datahub/shared/utils/graph/graph-transformations';
import { action, set, setProperties } from '@ember/object';
import { IGraphViewerState } from '@datahub/shared/types/graph/graph-viewer-state';
import { task } from 'ember-concurrency';
import { ETaskPromise } from '@datahub/utils/types/concurrency';
import { ISharedStateArgs } from '@datahub/shared/types/entity-page/components/entity-page-content/content-panel-with-toggle';
import RouterService from '@ember/routing/router-service';
import { extractEntityType } from '@datahub/utils/validators/urn';
import DataModelsService from '@datahub/data-models/services/data-models';
import { excludeNodes } from '@datahub/shared/utils/graph/graph-transformations';

/**
 * Closest is missing from TS definition
 */
type MyEventTarget = EventTarget & { closest: (str: string) => HTMLElement };
type MyMouseEvent = MouseEvent & { target: MyEventTarget };

/**
 * Base CSS Class
 */
const baseClass = 'graph-viewer';

/**
 * Design Doc:
 * https://docs.google.com/document/d/1R1-qIbJusGjXmZ_hlKA3KufQA9ufr7fjKSriVXATmCY/edit#heading=h.5x0d5h95i329
 *
 * This component will render a Graph type.
 *
 * GraphViz is the library that will render the svg. GraphViz is a C library compiled with
 * emscripten to use it in JS, and ported as Viz.js
 *
 * Viz.js includes a worker that renders the SVG, that's why we need a service to invoke it.
 *
 * We will transform the Graph Model into Dot notation (graphviz input type).
 * Once we have dot file, we will send it to GraphViz and we will get and svg string as response.
 *
 * We will convert the svg string into an svg element. We will transform svg element as the output
 * of graphviz is not suitable for interactions or styling. We will stripe out all inline style,
 * add css classes and some 'data-' inside the nodes for easy selection.
 *
 * Once this is done, we will use SVGPanZoom to handle the navigation in the svg.
 *
 */
export default class GraphViewer extends Component<ISharedStateArgs<IGraphViewerState>> {
  baseClass = baseClass;
  /**
   * Service to interact with GraphViz worker
   */
  @service
  graphRenderer!: GraphRenderer;

  /**
   * SvgPanZoom reference to navigate through the entities
   */
  svgPanZoom?: SvgPanZoom.Instance;

  /**
   * Max zoom for this graph. Depending on the width of the svg, this number will be calculated when rendering.
   * See calculateMaxZoom
   */
  maxZoom?: number;

  /**
   * Resize observer needed for fullscreen modes. It will watch svgContainer changes
   */
  resizeObserver: ResizeObserver = new ResizeObserver(() => this.onDomResize());

  /**
   * As svg is not rendered in ember, we need to create an alternative to easily bind events to elements.
   * ClickBinds will help you to do that by simulating a listener. See click() for implementation details
   */
  clickBinds: Record<string, (target: HTMLElement, node: HTMLElement, event: MouseEvent) => Promise<void>> = {
    [`.${baseClass}__title`]: this.onTitleClick,
    [`.${baseClass}__property-type--reference`]: this.onReferenceClick,
    [`.${baseClass}__action-go-to-entity`]: this.onGoToEntityClick
  };

  /**
   * Reference to SVG container for DOM manipulation purposes
   */
  svgContainer?: Element;

  /**
   * Flag to enable once all svg rendenring is done. This will help to ignore state changes until the graph is rendered.
   */
  rendered = false;

  /**
   * Will be needed to perform transitions
   */
  @service
  router?: RouterService;

  /**
   * DataModelsService will be used to generate links to entities when needed
   */
  @service('data-models')
  dataModels?: DataModelsService;

  /**
   * When new state check if some action needs to be performed
   */
  @action
  onUpdatedState(): void {
    if (this.rendered) {
      const { state, lastState } = this.args;

      // if selectedNode is different that lastState, then select entity
      if (state?.selectedNodeId && state?.selectedNodeId !== lastState?.selectedNodeId) {
        this.onChangeSelectedEntity();
      }

      // if show connector is different than lastState, then render graph again
      if (this.svgContainer && state?.showAttributes !== lastState?.showAttributes) {
        this.renderGraphTask.perform(this.svgContainer);
      }

      // if exludeNodePattern is different than lastState, then render graph again
      if (this.svgContainer && state?.excludeNodePattern !== lastState?.excludeNodePattern) {
        this.renderGraphTask.perform(this.svgContainer);
      }
    }
  }

  /**
   * Action handle when users click on View Entity
   * @param button element that contains the reference to go
   */
  onGoToEntityClick(button: HTMLElement): Promise<void> {
    const entityUrn = button.getAttribute('data-entity-urn');
    const apiEntityType = entityUrn && extractEntityType(entityUrn);
    const EntityModel = apiEntityType && this.dataModels?.getModelByApiName(apiEntityType);
    if (EntityModel && entityUrn) {
      const entityInstance = this.dataModels?.createPartialInstance(EntityModel?.displayName, entityUrn);
      const link = entityInstance?.entityLink;
      const { route, model } = (link && link.link) || {};
      const url = route && model && this.router?.urlFor.apply(this.router, [route, ...model]);
      url && window.open(url);
    }
    return Promise.resolve();
  }

  /**
   * Will select an entity in the state
   * @param nodeId id of the entity
   */
  selectEntity(nodeId: string): void {
    this.args.onStateChanged({
      ...this.args.state,
      selectedNodeId: nodeId
    });
  }

  /**
   *
   * Will add a css class to an edge
   *
   * @param fromNode edge from
   * @param toNode edge to
   * @param clazz class you wanted to add
   */
  addClassToEdge(fromNode: string, toNode: string, clazz: string): void {
    const { svgContainer } = this;
    if (svgContainer) {
      const edgeElement = svgContainer.querySelectorAll(`[data-to="${toNode}"][data-from="${fromNode}"]`);

      edgeElement.forEach((edge): void => edge.classList.add(clazz));
    }
  }

  /**
   * Will add a class to a node
   * @param id of the node
   * @param clazz class you wanted to add
   */
  addClassToNode(id: string, clazz: string): Element | null | void {
    const { svgContainer } = this;
    if (svgContainer) {
      const element = svgContainer.querySelector(`[data-entity-id="${id}"`);
      element?.classList.add(clazz);
      return element;
    }
  }

  /**
   * Will select a node highlighting the path to reach that node.
   * @param animate whether we need animation to focus on the node or not
   */
  async selectNodeForLineage(animate: boolean): Promise<void> {
    const { svgPanZoom, svgContainer, maxZoom = 0 } = this;
    const { selectedNodeId, graph } = this.args.state || {};
    const nodeId = selectedNodeId;
    const node = svgContainer?.querySelector(`[data-entity-id="${nodeId}"`);

    if (nodeId && svgContainer && graph && graph.rootNode && node) {
      const downstreams = getAllPaths(graph, graph.rootNode, nodeId);
      const upstreams = getAllPaths(graph, nodeId, graph.rootNode);
      const allDownstreamNodes = downstreams.reduce((nodes, downstream) => [...nodes, ...downstream.nodes], []);
      const allUpstreamNodes = upstreams.reduce((nodes, upstream) => [...nodes, ...upstream.nodes], []);
      const elements: Array<Element> = [...allDownstreamNodes, ...allUpstreamNodes]
        .map(node => this.addClassToNode(node, 'node-related'))
        .filter(Boolean) as Array<Element>;

      downstreams.forEach(downstream =>
        downstream.edges.forEach(edge => this.addClassToEdge(edge.fromNode, edge.toNode, 'edge-from-selected'))
      );

      upstreams.forEach(upstream =>
        upstream.edges.forEach(edge => this.addClassToEdge(edge.fromNode, edge.toNode, 'edge-to-selected'))
      );

      node.classList.add('selected');

      svgPanZoom &&
        svgContainer &&
        (await focusElements(elements.length > 0 ? elements : [node], svgContainer, svgPanZoom, maxZoom, animate));
    }
  }

  /**
   * Will highlight the selected node and the outgoing and incoming edges from the node
   * @param animate whether we need animation to focus on the node or not
   */
  async selectNodeDefault(animate: boolean): Promise<void> {
    const { svgPanZoom, svgContainer, maxZoom = 0 } = this;
    const { selectedNodeId, graph } = this.args.state || {};
    const nodeId = selectedNodeId;
    const node = svgContainer?.querySelector(`[data-entity-id="${nodeId}"`);

    if (nodeId && svgContainer && graph && node) {
      forEachNode(svgContainer, `[data-from="${nodeId}"]`, (edge): void => edge.classList.add('edge-from-selected'));
      forEachNode(svgContainer, `[data-to="${nodeId}"]`, (edge): void => edge.classList.add('edge-to-selected'));
      forEachNode(svgContainer, `[data-reference="${nodeId}"]`, (attribute): void => {
        const relatedNode = attribute && attribute.closest('.node');
        relatedNode && relatedNode.classList.add('node-related');
        attribute.classList.add('attr-selected');
      });

      node.classList.add('selected');

      svgPanZoom && svgContainer && (await focusElements([node], svgContainer, svgPanZoom, maxZoom, animate));
    }
  }
  /**
   * Will select an entity using an Id
   * @param nodeId id of the entity
   */
  async onChangeSelectedEntity(animate = true): Promise<void> {
    const { svgContainer } = this;
    const { selectedNodeId, graph, lineageMode = false } = this.args.state || {};
    const nodeId = selectedNodeId;

    if (nodeId && graph) {
      // Clear previous selections
      forEachNode(svgContainer, '.edge-to-selected', (el): void => el.classList.remove('edge-to-selected'));
      forEachNode(svgContainer, '.edge-from-selected', (el): void => el.classList.remove('edge-from-selected'));
      forEachNode(svgContainer, '.selected', (el): void => el.classList.remove('selected'));
      forEachNode(svgContainer, '.attr-selected', (el): void => el.classList.remove('attr-selected'));
      forEachNode(svgContainer, '.node-related', (el): void => el.classList.remove('node-related'));

      if (lineageMode) {
        await this.selectNodeForLineage(animate);
      } else {
        await this.selectNodeDefault(animate);
      }
    }
  }

  /**
   * Handle for clicking on the title of an entity
   * @param _title title element (not used)
   * @param node node that the title belongs to
   */
  async onTitleClick(_title: HTMLElement, node: HTMLElement): Promise<void> {
    const nodeId = node.getAttribute('data-entity-id');
    if (nodeId) {
      await this.selectEntity(nodeId);
    }
  }

  /**
   * Handle when a reference is clicked
   * @param reference reference element clicked
   */
  async onReferenceClick(reference: HTMLElement): Promise<void> {
    const nodeId = reference.getAttribute('data-reference-link');
    if (nodeId) {
      await this.selectEntity(nodeId);
    }
  }

  /**
   * Generic click handler that will use clickBinds to invoke the corresponding method
   * if selector matches
   * @param event
   */
  @action
  async click(event: MyMouseEvent): Promise<void> {
    const { target } = event;
    const { clickBinds } = this;
    if (target) {
      const node = target.closest('.node');

      await Promise.all(
        Object.keys(clickBinds).map(
          async (selector: keyof typeof clickBinds): Promise<void> => {
            const selectedElement = target.closest(selector);
            selectedElement && (await clickBinds[selector].apply(this, [selectedElement, node, event]));
          }
        )
      );
    }
  }

  /**
   * Will create the svg element and attach it to the dom.
   * See description of component
   */
  @action
  renderGraph(svgContainer: Element): Promise<void> {
    const { selectedNodeId, showAttributes = false, graph } = this.args.state || {};
    this.clearResources();
    setProperties(this, {
      svgContainer
    });
    this.args.onStateChanged({
      ...this.args.state,
      selectedNodeId: selectedNodeId || graph?.rootNode,
      showAttributes
    });
    return this.paintGraph();
  }

  /**
   * Task wrapper around renderGraph. This will allow us to use both, task interface or promise interface.
   */
  @(task(function*(this: GraphViewer, svgContainer: Element): IterableIterator<Promise<void>> {
    yield this.renderGraph(svgContainer);
  }).restartable())
  renderGraphTask!: ETaskPromise<void, Element>;

  /**
   * Handler when svgContainer changes in size
   */
  onDomResize(): void {
    const svg = this.svgContainer?.querySelector('svg');
    const { clientWidth, clientHeight } = this.svgContainer || {};
    svg && svg.setAttribute('width', `${clientWidth}px`);
    svg && svg.setAttribute('height', `${clientHeight}px`);
    this.svgPanZoom?.resize();
  }

  /**
   * Will clean HTML and memory references
   */
  @action
  clearResources(): void {
    const { svgContainer, svgPanZoom } = this;

    if (svgContainer) {
      this.resizeObserver.unobserve(svgContainer);
      // apparently this is the more efficient approach
      while (svgContainer.firstChild) {
        svgContainer.removeChild(svgContainer.firstChild);
      }

      set(this, 'svgContainer', undefined);
    }

    if (svgPanZoom) {
      svgPanZoom.destroy();

      set(this, 'svgPanZoom', undefined);
    }

    set(this, 'rendered', false);
  }

  /**
   * For integration purposes, we need to remove the attribute
   * 'viewBox' from the reference icon svg
   * @param svg
   */
  cleanIconSvg(svg: SVGElement): void {
    svg.removeAttribute('viewBox');
  }

  /**
   * Will create the graph in the dom
   * See description of component
   */
  async paintGraph(): Promise<void> {
    const { graphRenderer, svgContainer, resizeObserver } = this;
    const { state } = this.args;
    const { showAttributes = false, graph, lineageMode = false, fullscreenMode = false, excludeNodePattern } =
      state || {};
    if (graph && svgContainer) {
      const { clientWidth, clientHeight } = svgContainer;
      const fixedGraph = moveAttributeEdgesToEdges(graph);
      const graphWithoutExludedNodes = excludeNodes(fixedGraph, excludeNodePattern);
      const graphWithoutSimpleAttributes = showAttributes
        ? graphWithoutExludedNodes
        : excludeSimpleAttributes(graphWithoutExludedNodes);
      const str = await graphRenderer.render(graphWithoutSimpleAttributes, { lineageMode });
      const svgDoc = new DOMParser().parseFromString(str, 'image/svg+xml');
      const processedSvg = processGraphVizSvg((svgDoc.documentElement as unknown) as SVGSVGElement, baseClass);
      const svg = svgContainer.appendChild(processedSvg);

      this.maxZoom = calculateMaxZoom(svg);
      svg.setAttribute('width', `${clientWidth}px`);
      svg.setAttribute('height', `${clientHeight}px`);

      this.svgPanZoom = svgPanZoom(svg, {
        zoomScaleSensitivity: 0.25,
        zoomEnabled: true,
        controlIconsEnabled: true,
        fit: true,
        center: true,
        minZoom: 0.95,
        maxZoom: this.maxZoom
      });

      if (fullscreenMode) {
        resizeObserver.observe(svgContainer);
      }

      set(this, 'rendered', true);

      this.onChangeSelectedEntity(false);

      this.args.onStateChanged({ ...this.args.state, numberShowingNodes: graphWithoutExludedNodes.nodes.length });
    }
  }
}
