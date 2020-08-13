export interface IDynamicLinkNode<ModelType = undefined, Route = string, QueryParams extends {} = {}> {
  title: string;
  text: string;
  route: Route;
  model?: ModelType;
  queryParams?: QueryParams;
}
