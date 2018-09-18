import SearchController from 'wherehows-web/controllers/search';

declare module '@ember/controller' {
  // eslint-disable-next-line typescript/interface-name-prefix
  interface Registry {
    search: SearchController;
  }
}
