import { IFunctionRouteHandler, IMirageServer } from 'wherehows-web/typings/ember-cli-mirage';
import { ApiStatus } from 'wherehows-web/utils/api/shared';

export default function(this: IMirageServer) {
  this.passthrough('/config', '/authenticate');

  this.namespace = '/api/v1';

  this.get('/datasets/:id/compliance/suggestions', function(
    this: IFunctionRouteHandler,
    { complianceSuggestions }: { complianceSuggestions: any }
  ) {
    return {
      status: ApiStatus.OK,
      autoClassification: {
        urn: '',
        classificationResult: JSON.stringify(this.serialize(complianceSuggestions.all())),
        lastModified: new Date().getTime()
      }
    };
  });

  this.passthrough();
}
