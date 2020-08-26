import { TestContext } from 'ember-test-helpers';

// Mirage deals with highly variable entities and we should use any for now to capture functions
// properly and allow the community to perhaps eventually work on more sophisticated typings
/*eslint-disable @typescript-eslint/no-explicit-any */

/**
 * Defines the available interface on a Mirage server.
 * This is not an exhaustive list but exposes some of the api that's used within the app
 *
 * T is the list of databases available on your specific app
 */
// eslint-disable-next-line  @typescript-eslint/no-explicit-any
export interface IMirageServer<T extends IMirageDBs = any> {
  db: T;
  options: object;
  urlPrefix: string;
  namespace: string;
  timing: number;
  logging: boolean;
  pretender: object;
  environment: string;

  // eslint-disable-next-line  @typescript-eslint/no-explicit-any
  get: (this: IMirageServer, path: string, ...args: Array<any>) => void;

  // eslint-disable-next-line  @typescript-eslint/no-explicit-any
  post: (this: IMirageServer, path: string, ...args: Array<any>) => void;

  // eslint-disable-next-line  @typescript-eslint/no-explicit-any
  put: (this: IMirageServer, path: string, ...args: Array<any>) => void;

  // eslint-disable-next-line  @typescript-eslint/no-explicit-any
  delete: (this: IMirageServer, path: string, ...args: Array<any>) => void;

  // eslint-disable-next-line  @typescript-eslint/no-explicit-any
  del: (this: IMirageServer, path: string, ...args: Array<any>) => void;

  // eslint-disable-next-line  @typescript-eslint/no-explicit-any
  patch: (this: IMirageServer, path: string, ...args: Array<any>) => void;

  // eslint-disable-next-line  @typescript-eslint/no-explicit-any
  head: (this: IMirageServer, path: string, ...args: Array<any>) => void;
  isTest: (this: IMirageServer) => boolean;
  passthrough: (...paths: Array<string>) => void;
  loadFixtures: (...files: Array<string>) => void;
  loadFactories: (factoryMap: object) => void;
  create: (type: string, options?: object | number) => object;
  createList: <T>(type: string, amount: number, traitsAndOverrides?: object | string) => Array<T>;
  shutdown: (this: IMirageServer) => void;
}

/**
 * Describes the public interface for the IBaseRouteHandler
 */
interface IBaseRouteHandler {
  getModelClassFromPath: (fullPath: string) => string;
}

/**
 * Describes the interface for the IFunctionRouteHandler: the execution context for route handlers in
 * Mirage's config file - mirage/config.ts
 */
export interface IFunctionRouteHandler extends IBaseRouteHandler {
  // eslint-disable-next-line  @typescript-eslint/no-explicit-any
  handle: (request: any) => any;

  // eslint-disable-next-line  @typescript-eslint/no-explicit-any
  setRequest: (request: any) => void;

  // eslint-disable-next-line  @typescript-eslint/no-explicit-any
  serialize: (response: any, serializerType?: string) => any;

  // eslint-disable-next-line  @typescript-eslint/no-explicit-any
  normalizedRequestAttrs: () => any;
}

/**
 * Mirage function handler request params
 * T is the queryParams
 */

// eslint-disable-next-line  @typescript-eslint/no-explicit-any
export interface IMirageRequest<T = any, Q = any> {
  status: number;
  queryParams: T;
  params?: Q;
  requestBody: string;
}

/**
 * When querying dbs, mirage returns this type of structure.
 * Objects are inside models.
 */
export interface IMirageDBQueryResult<T> {
  models: Array<T>;
}

/**
 * Mirage DB spec
 *
 * T is the type of the entity returned in the DB
 */
export interface IMirageDB<T> {
  all: () => IMirageDBQueryResult<T>;
  insert: (entries: Array<T>) => void;
  where: (query: Partial<T> | ((obj: T) => boolean)) => IMirageDBQueryResult<T>;
  first: () => IMirageDBQueryResult<T> | T;
  findBy: (query: Partial<T>) => IMirageDBQueryResult<T>;
}

/**
 * Generic list of BDs. Your app should implement your own specific list
 */
export interface IMirageDBs {
  // eslint-disable-next-line  @typescript-eslint/no-explicit-any
  [key: string]: IMirageDB<any>;
}

/**
 * Interface for a mirage test context vs a normal test context, helpful for when a test requires
 * instantiation of mirage, but typescript errors out because we are missing a "server" context
 */
export interface IMirageTestContext extends TestContext {
  server: IMirageServer;
}
