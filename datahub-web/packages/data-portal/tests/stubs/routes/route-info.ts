import { RouteInfo, RouteInfoWithAttributes } from 'wherehows-web/typings/modules/routerjs';

/**
 * Define a stub class for a RouteInfo object
 * @class RouteInfoStub
 * @implements {RouteInfoWithAttributes}
 */
export class RouteInfoStub implements RouteInfoWithAttributes {
  attributes = {};
  find(_arg0: (this: any, { name }: RouteInfo, i: number) => boolean): RouteInfo | undefined {
    return name === this.name ? this : undefined;
  }
  constructor(
    readonly name: string,
    attrs: object,
    readonly parent = null,
    readonly child = null,
    readonly localName: string = name,
    readonly params = {},
    readonly paramNames = [],
    readonly queryParams = {},
    readonly metadata = {}
  ) {
    this.attributes = attrs;
  }
}
