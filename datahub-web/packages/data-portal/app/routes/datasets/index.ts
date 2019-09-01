import Route from '@ember/routing/route';

export default class DatasetsIndex extends Route {
  redirect() {
    // default transition to datasets route if user enters through index
    return this.transitionTo('browse.entity', 'datasets');
  }
}
