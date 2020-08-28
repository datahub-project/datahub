import Component from '@ember/component';
import { task, Task } from 'ember-concurrency';
import { DatasetLineageList, IDatasetLineage } from '@datahub/metadata-types/types/entity/dataset/lineage';
import { readDownstreamDatasetsByUrn, readUpstreamDatasetsByUrn } from 'datahub-web/utils/api/datasets/lineage';
import { containerDataSource } from '@datahub/utils/api/data-source';
import GraphDb, { INode } from 'datahub-web/utils/graph-db';
import { action } from '@ember/object';
import { ETaskPromise } from '@datahub/utils/types/concurrency';
import { IDatasetEntity } from '@datahub/metadata-types/types/entity/dataset/dataset-entity';

/**
 * Container component for the lineage graph. Will use upstream and downstream to generate the graph
 */
@containerDataSource<DatasetRelationshipLevels>('firstLoadTask', ['dataset'])
export default class DatasetRelationshipLevels extends Component {
  /**
   * GraphDb will be the one contain the nodes and edges. It will help us to generate the graph
   */
  graphDb: GraphDb<IDatasetLineage> = GraphDb.create({
    uniqueKeys: ['dataset.uri']
  }) as GraphDb<IDatasetLineage>;

  /**
   * This is the dataset that we need to show upstream and downstream data
   */
  dataset!: IDatasetEntity;

  /**
   * This task will fetch lineage data for a node. If a node is downtream, it will load children
   * If the node is upstream, it will fetch its parents.
   * @param id
   * @param upstream
   */
  @task(function*(
    this: DatasetRelationshipLevels,
    id: number,
    upstream = false
  ): IterableIterator<Promise<DatasetLineageList>> {
    const currentNode = this.graphDb.nodesById[id];
    if (currentNode) {
      const method = upstream ? readUpstreamDatasetsByUrn : readDownstreamDatasetsByUrn;
      const nodes = ((yield method(
        (currentNode.payload && currentNode.payload.dataset.uri) || ''
      )) as unknown) as Array<IDatasetLineage>;
      nodes.forEach((node): INode<IDatasetLineage> => this.graphDb.addNode(node, currentNode, upstream));
      this.graphDb.setNodeAttrs(id, {
        loaded: true
      });
    }
  })
  getLineageDataTask: Task<Promise<DatasetLineageList>, (a: number, b?: boolean) => Promise<DatasetLineageList>>;

  /**
   * It will create the first node using the dataset passed, and invoke upstream and downstream tasks
   */
  @task(function*(this: DatasetRelationshipLevels): IterableIterator<Promise<DatasetLineageList>> {
    const rootNode = this.graphDb.addNode({
      dataset: this.dataset,
      type: '',
      actor: ''
    });

    yield this.getLineageDataTask.perform(rootNode.id, false);

    yield this.getLineageDataTask.perform(rootNode.id, true);

    //select first node
    this.graphDb.setNodeAttrs(rootNode.id, {
      selected: true
    });
  })
  firstLoadTask!: ETaskPromise<DatasetLineageList>;

  /**
   * When we select a node, we need to toggle it and load it if necessary
   * @param node
   */
  @action
  toggleNode(node: INode<IDatasetLineage>): void {
    this.graphDb.toggle(node.id);
    if (!node.loaded) {
      this.getLineageDataTask.perform(node.id, node.level < 0);
    }
  }
}
