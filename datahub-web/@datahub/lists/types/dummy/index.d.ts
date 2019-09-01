declare module 'dummy/app';
declare module 'ember-cli-mirage';

declare module 'dummy/initializers/ember-cli-mirage' {
  import { IMirageServer } from '@datahub/utils/types/vendor/ember-cli-mirage-deprecated';

  export const startMirage: (env?: string) => IMirageServer;
}
