// Declares the startMirage exported function interface
declare module 'dummy/initializers/ember-cli-mirage' {
  import { Server } from 'ember-cli-mirage';

  export const startMirage: (env?: string) => Server;
}
