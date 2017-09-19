declare module 'ember-modal-dialog/components/modal-dialog';

declare module 'ember-simple-auth/mixins/authenticated-route-mixin';

declare module 'wherehows-web/utils/datasets/functions';

// https://github.com/ember-cli/ember-fetch/issues/72
// TS assumes the mapping btw ES modules and CJS modules is 1:1
// However, `ember-fetch` is the module name, but it's imported with `fetch`
declare module 'fetch' {
  export default function fetch(input: RequestInfo, init?: RequestInit): Promise<Response>;
}
