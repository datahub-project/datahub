import Session from 'ember-simple-auth/services/session';
import CurrentUser from '@datahub/shared/services/current-user';
import Metrics from 'ember-metrics';
import BannerService from 'datahub-web/services/banners';
import HotKeys from 'datahub-web/services/hot-keys';
import Search from '@datahub/shared/services/search';

declare module '@ember/service' {
  // eslint-disable-next-line @typescript-eslint/interface-name-prefix
  interface Registry {
    search: Search;
    session: Session;
    metrics: Metrics;
    banners: BannerService;
    'current-user': CurrentUser;
    'hot-keys': HotKeys;
  }
}
