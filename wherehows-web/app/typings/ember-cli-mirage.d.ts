/**
 * Defines the available interface on a Mirage server.
 * This is not an exhaustive list but exposes some of the api that's used within the app
 */
export interface IMirageServer {
  options: object;
  urlPrefix: string;
  namespace: string;
  timing: number;
  logging: boolean;
  pretender: object;
  environment: string;
  get: (this: IMirageServer, path: string, ...args: Array<any>) => void;
  post: (this: IMirageServer, path: string, ...args: Array<any>) => void;
  put: (this: IMirageServer, path: string, ...args: Array<any>) => void;
  delete: (this: IMirageServer, path: string, ...args: Array<any>) => void;
  del: (this: IMirageServer, path: string, ...args: Array<any>) => void;
  patch: (this: IMirageServer, path: string, ...args: Array<any>) => void;
  head: (this: IMirageServer, path: string, ...args: Array<any>) => void;
  isTest: (this: IMirageServer) => boolean;
  passthrough: (...paths: Array<string>) => void;
  loadFixtures: (...files: Array<string>) => void;
  loadFactories: (factoryMap: object) => void;
  create: (type: string, options?: object) => object;
  createList: <T>(type: string, amount: number, traitsAndOverrides?: object) => Array<T>;
  shutdown: (this: IMirageServer) => void;
}
