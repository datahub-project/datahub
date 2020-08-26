// once this is released, the declaration can be removed, tsconfig contains the proposed update
// https://github.com/ember-cli/ember-fetch/pull/136/files
// original issue here:
// https://github.com/ember-cli/ember-fetch/issues/72
// TS assumes the mapping btw ES modules and CJS modules is 1:1
// However, `ember-fetch` is the module name, but it's imported with `fetch`
declare module 'fetch' {
  export default function fetch(
    input: RequestInfo,
    options?: { method?: string; body?: object; headers?: object | Headers; credentials?: RequestCredentials }
  ): Promise<Response>;
}
