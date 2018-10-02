declare module '@ember/service' {
  import Session from 'ember-simple-auth/services/session';
  import CurrentUser from 'wherehows-web/services/current-user';
  import Metrics from 'ember-metrics';
  import BannerService from 'wherehows-web/services/banners';
  import Notifications from 'wherehows-web/services/notifications';
  import UserLookup from 'wherehows-web/services/user-lookup';
  import HotKeys from 'wherehows-web/services/hot-keys';
  import Search from 'wherehows-web/services/search';

  // eslint-disable-next-line typescript/interface-name-prefix
  interface Registry {
    search: Search;
    session: Session;
    metrics: Metrics;
    banners: BannerService;
    notifications: Notifications;
    'current-user': CurrentUser;
    'user-lookup': UserLookup;
    'hot-keys': HotKeys;
  }
}
