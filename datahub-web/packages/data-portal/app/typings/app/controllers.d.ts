import SearchController from 'datahub-web/controllers/search';

declare module '@ember/controller' {
  // eslint-disable-next-line @typescript-eslint/interface-name-prefix
  interface Registry {
    search: SearchController;
  }
}
