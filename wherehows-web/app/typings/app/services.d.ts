declare module '@ember/service' {
  import Session from 'ember-simple-auth/services/session';
  import CurrentUser from 'wherehows-web/services/current-user';
  import Metrics from 'ember-metrics';
  import Search from 'wherehows-web/services/search';
  import BannerService from 'wherehows-web/services/banners';
  import Notifications from 'wherehows-web/services/notifications';
  import UserLookup from 'wherehows-web/services/user-lookup';

  // eslint-disable-next-line typescript/interface-name-prefix
  interface Registry {
    session: Session;
    metrics: Metrics;
    search: Search;
    banners: BannerService;
    notifications: Notifications;
    'current-user': CurrentUser;
    'user-lookup': UserLookup;
  }
}
