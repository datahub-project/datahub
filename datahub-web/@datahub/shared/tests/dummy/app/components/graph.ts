import Component from '@glimmer/component';
import { IGraphViewerState } from '@datahub/shared/types/graph/graph-viewer-state';
import { rootNode } from '../constants/lineage';
import { simpleGraph } from '../constants/schema';
import { setProperties, action } from '@ember/object';
import { tracked } from '@glimmer/tracking';
export default class Graph extends Component<{
  lineageMode: boolean;
}> {
  @tracked
  state: IGraphViewerState = {
    graph: this.args.lineageMode ? rootNode : simpleGraph,
    showAttributes: false,
    lineageMode: this.args.lineageMode
  };
  @tracked
  lastState: IGraphViewerState | undefined;

  @action
  onStateChanged(newState: IGraphViewerState): void {
    const lastState = this.state;
    const state = newState;
    setProperties(this, {
      state,
      lastState
    });
  }
}
