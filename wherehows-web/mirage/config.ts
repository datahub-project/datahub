import { IMirageServer } from 'wherehows-web/typings/ember-cli-mirage';

export default function(this: IMirageServer) {
  this.namespace = '/api/v1/';

  this.get('/compliance/suggestions', () => {});

  this.passthrough();
}
