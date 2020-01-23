export interface IDynamicLinkNode<T, Z = string, P extends {} = {}> {
  title: string;
  text: string;
  route: Z;
  model?: T;
  queryParams?: P;
}
