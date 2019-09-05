import Session from 'ember-simple-auth/services/session';
import CurrentUser from '@datahub/shared/services/current-user';
import Metrics from 'ember-metrics';
import BannerService from 'wherehows-web/services/banners';
import UserLookup from 'wherehows-web/services/user-lookup';
import HotKeys from 'wherehows-web/services/hot-keys';
import Search from 'wherehows-web/services/search';

declare module '@ember/service' {
  // eslint-disable-next-line @typescript-eslint/interface-name-prefix
  interface Registry {
    search: Search;
    session: Session;
    metrics: Metrics;
    banners: BannerService;
    'current-user': CurrentUser;
    'user-lookup': UserLookup;
    'hot-keys': HotKeys;
  }
}
