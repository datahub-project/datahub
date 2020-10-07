/* eslint-disable @typescript-eslint/no-explicit-any */

declare module 'datahub-web/app';

declare module 'datahub-web/initializers/ember-cli-mirage' {
  import { IMirageServer } from '@datahub/utils/types/vendor/ember-cli-mirage-deprecated';

  export const startMirage: (env?: string) => IMirageServer;
}

declare module 'ivy-tabs/components/ivy-tabs-tablist' {
  import Component from '@ember/component';
  export default class IvyTabsTablistComponent extends Component {
    focusSelectedTab: () => void;
    selectPreviousTab: () => void;
    selectNextTab: () => void;
  }
}

/**
 * Merges global type defs for global modules on the window namespace.
 * These should be refactored into imported modules if available or shimmed as such
 */
// eslint-disable-next-line @typescript-eslint/interface-name-prefix
interface Window {
  JsonHuman: {
    format(
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      data: any,
      options?: {
        showArrayIndex: void | boolean;
        hyperlinks: { enable: boolean; keys: null | Array<string>; target: string };
        bool: {
          //https://github.com/prettier/prettier/issues/3102
          text: object; //{ 'true': string; 'false': string };
          image: object; //{ 'true': string; 'false': string };
          showImage: boolean;
          showText: boolean;
        };
      }
    ): Element;
  };
}
