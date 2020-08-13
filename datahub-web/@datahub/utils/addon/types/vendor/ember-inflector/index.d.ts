declare module 'ember-inflector' {
  const singularize: (arg: string) => string;
  export function pluralize(arg: string): string;
  export function pluralize(count: number, arg: string, options?: { withoutCount: boolean }): string;
}
