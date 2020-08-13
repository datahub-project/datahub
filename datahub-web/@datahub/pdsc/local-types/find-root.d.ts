declare module 'find-root' {
  function findRoot(start?: string, check?: (file: string) => boolean): string | undefined;
  export = findRoot;
}
