import Controller from '@ember/controller';

export default class Application extends Controller {
  entity = {
    urn: 'entityUrn',
    formula: 'sum(job_apply_click)'
  };
}

// DO NOT DELETE: this is how TypeScript knows how to look up your controllers.
declare module '@ember/controller' {
  // eslint-disable-next-line @typescript-eslint/interface-name-prefix
  interface Registry {
    application: Application;
  }
}
