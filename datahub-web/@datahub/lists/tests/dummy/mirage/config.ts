import {
  IMirageServer,
  IMirageRequest,
  IFunctionRouteHandler
} from '@datahub/utils/types/vendor/ember-cli-mirage-deprecated';

export default function(this: IMirageServer) {
  this.namespace = '/api/v2';

  this.get('/features/:urn', function(this: IFunctionRouteHandler, _schema: unknown, request: IMirageRequest) {
    return { urn: request.params.urn };
  });

  this.get('/features/snapshots/:urns', function(
    this: IFunctionRouteHandler,
    _schema: unknown,
    request: IMirageRequest
  ) {
    return request.params.urns.split(';').map((urn: string) => ({ urn }));
  });
}
