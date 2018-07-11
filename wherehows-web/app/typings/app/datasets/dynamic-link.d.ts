import { IBrowserRouteParams } from 'wherehows-web/routes/browse/entity';
import { IReadDatasetsOptionBag } from 'wherehows-web/typings/api/datasets/dataset';

interface IDynamicLinkNode {
  title: string;
  text: string;
  route: 'browse.entity' | 'datasets.dataset';
  model: IBrowserRouteParams['entity'];
  queryParams?: Partial<IReadDatasetsOptionBag>;
}

export { IDynamicLinkNode };
