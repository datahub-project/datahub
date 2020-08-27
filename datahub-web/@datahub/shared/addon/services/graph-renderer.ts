import Service from '@ember/service';
import { graphToDot } from '@datahub/shared/utils/graph/graph-to-dot';
import { getOwner } from '@ember/application';
import Viz from '@aduh95/viz.js';
import { IGraphOptions } from '@datahub/shared/types/graph/graph-options';

type Graph = Com.Linkedin.Metadata.Graph.Graph;

/**
 * Service to handle Viz worker comunications
 */
export default class GraphRenderer extends Service {
  /**
   * Viz instance
   */
  viz!: Viz;

  /**
   * Will create Viz worker
   */
  init(): void {
    super.init();

    const { rootURL } = getOwner(this).resolveRegistration('config:environment');

    this.viz = new Viz({
      workerURL: `${rootURL === '/' ? '' : rootURL}/@datahub/shared/assets/viz-worker.js`
    });
  }

  /**
   * Will transform graph to dot notation, send it to viz and return a string with the svg
   * @param graph graph to be rendered
   */
  async render(graph: Graph, options?: IGraphOptions): Promise<string> {
    const dot = graphToDot(graph, options);
    const str = await this.viz.renderString(dot);
    return str;
  }
}

// DO NOT DELETE: this is how TypeScript knows how to look up your services.
declare module '@ember/service' {
  // eslint-disable-next-line
  interface Registry {
    'graph-renderer': GraphRenderer;
  }
}
