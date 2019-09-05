/* eslint-disable @typescript-eslint/no-explicit-any */

declare module 'wherehows-web/app';

declare module 'ember-simple-auth/mixins/application-route-mixin' {
  import MixinTwo from '@ember/object/mixin';
  export default MixinTwo;
}

declare module 'wherehows-web/initializers/ember-cli-mirage' {
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

declare module 'ember-simple-auth/authenticators/base' {
  import EmberObject from '@ember/object';
  export default EmberObject;
}

declare module 'ember-inflector' {
  const singularize: (arg: string) => string;
  export function pluralize(arg: string): string;
  export function pluralize(count: number, arg: string, options?: { withoutCount: number }): string;
}

declare module 'ember-cli-mirage';

// https://github.com/ember-cli/ember-fetch/issues/72
// TS assumes the mapping btw ES modules and CJS modules is 1:1
// However, `ember-fetch` is the module name, but it's imported with `fetch`
declare module 'fetch' {
  export default function fetch(
    input: RequestInfo,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    options?: { method?: string; body?: any; headers?: object | Headers; credentials?: RequestCredentials }
  ): Promise<Response>;
}

// Marked
interface IMarkedRenderer {
  link: (href: string, title: string, text: string) => string;
}

interface IMarkedOptions {
  gfm: boolean;
  tables: boolean;
  renderer: IMarkedRenderer;
}

type IMarkedCreate = (param: string) => { htmlSafe: () => string };

type Marked = IMarkedCreate & {
  Renderer: { new (): IMarkedRenderer };
  setOptions: (options: Partial<IMarkedOptions>) => void;
};

/**
 * Merges global type defs for global modules on the window namespace.
 * These should be refactored into imported modules if available or shimmed as such
 */
// eslint-disable-next-line @typescript-eslint/interface-name-prefix
interface Window {
  marked: Marked;

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

/**
 * Merges the JSONView plugin into the jquery interface
 */
// eslint-disable-next-line @typescript-eslint/interface-name-prefix
interface JQuery {
  JSONView(json: object): this;
  treegrid(): this;
}
