/* eslint-disable @typescript-eslint/interface-name-prefix*/
declare module 'ember-data/types/registries/model' {
  export default interface ModelRegistry {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    __UNUSED_KEY__: any;
  }
}

declare module 'ember-cli-mirage' {
  import { IMirageModelRegistry } from 'ember-cli-mirage/types/registries/model';
  import { IMirageSchemaRegistry } from 'ember-cli-mirage/types/registries/schema';
  import DS from 'ember-data';
  import EmberDataModelRegistry from 'ember-data/types/registries/model';
  import EmberObject from '@ember/object';
  import Faker from 'faker';

  export const faker: Faker.FakerStatic & {
    list: {
      random: <T>(...args: Array<T>) => T;
      cycle: <T>(...args: Array<T>) => T;
    };
  };

  export type MirageID = number | string;

  type MirageRecord<T> = T & { id: MirageID };

  // Query type is specified as a local convenience alias for Partial<T>, not expected as an external argument
  export interface DatabaseCollection<T = {}, Query = Partial<T>> {
    insert<S extends T | Array<T>>(data: S): S extends T ? MirageRecord<T> : Array<MirageRecord<T>>;
    find<S extends MirageID | Array<MirageID>>(ids: S): S extends MirageID ? MirageRecord<T> : Array<MirageRecord<T>>;
    findBy(query: Query): MirageRecord<T>;
    where(query: Query | ((r: MirageRecord<Query>) => boolean)): Array<MirageRecord<T>>;
    update(attrs: Query): Array<MirageRecord<T>>;
    update(target: MirageID | Query, attrs: Query): Array<MirageRecord<T>>;
    remove(target?: MirageID | Query): void;
    firstOrCreate(query: Query, attributesForCreate?: T): MirageRecord<T>;
    readonly length: number;
  }

  export type Database = {
    createCollection: (collectionName: string) => void;
    // loadData allows us to load a fixture directly onto the DB, currently being used since we
    // are having some trouble using loadFixtures() in an addon
    // https://www.ember-cli-mirage.com/docs/data-layer/fixtures
    loadData: (data: Partial<Record<keyof IMirageSchemaRegistry, unknown>>) => void;
  } & {
    [collectionName: string]: DatabaseCollection;
  } & { [collectionName in keyof IMirageSchemaRegistry]: DatabaseCollection<IMirageSchemaRegistry[collectionName]> };

  export type Model<T> = {
    [P in keyof T]: T[P] extends DS.Model & DS.PromiseObject<infer M>
      ? ModelInstance<M>
      : T[P] extends DS.Model
      ? ModelInstance<T[P]>
      : T[P] extends DS.PromiseManyArray<infer M>
      ? Collection<M>
      : T[P] extends Array<DS.Model> & DS.PromiseManyArray<infer M>
      ? Collection<M>
      : T[P] extends Array<DS.Model>
      ? Collection<T[P]>
      : T[P] extends Date
      ? Date | string
      : T[P];
  };

  export class ModelClass {
    extend<T>(attrs: ModelAttrs<T>): ModelClass;
  }

  export const Model: ModelClass;

  interface ModelInstanceShared<T> {
    id: MirageID;
    attrs: T;
    _schema: Schema;

    save(): void;
    update<K extends keyof T>(key: K, val: T[K]): void;
    update<K extends keyof T>(attrs: { [key in K]: T[K] }): void;
    destroy(): void;
    isNew(): boolean;
    isSaved(): boolean;
    reload(): void;
    toString(): string;
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  export type ModelInstance<T = any> = ModelInstanceShared<T> & Model<T>;

  export interface Collection<T> {
    models: Array<ModelInstance<T>>;
    length: number;
    modelName: string;
    firstObject: ModelInstance<T>;
    update<K extends keyof T>(key: K, val: T[K]): void;
    update<K extends keyof T>(attrs: { [key in K]: T[K] }): void;
    save(): void;
    reload(): void;
    destroy(): void;
    sort(sortFn: (a: ModelInstance<T>, b: ModelInstance<T>) => number): Collection<T>;
    filter(filterFn: (model: ModelInstance<T>) => boolean): Collection<T>;
  }
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  interface ModelClass<T = any> {
    new (attrs: Partial<ModelAttrs<T>>): ModelInstance<T>;
    create(attrs: Partial<ModelAttrs<T>>): ModelInstance<T>;
    update(attrs: Partial<ModelAttrs<T>>): ModelInstance<T>;
    all(): Collection<T>;
    find<S extends MirageID | Array<MirageID>>(ids: S): S extends MirageID ? ModelInstance<T> : Collection<T>;
    findBy(query: Partial<ModelAttrs<T>>): ModelInstance<T>;
    first(): ModelInstance<T>;
    where(query: Partial<ModelAttrs<T>> | ((r: ModelInstance<T>) => boolean)): Collection<T>;
  }

  export type Schema = { [modelName in keyof IMirageSchemaRegistry]: ModelClass<IMirageSchemaRegistry[modelName]> } & {
    db: Database;
  } & {
    [modelName: string]: ModelClass;
  };

  export class Response {
    constructor(code: number, headers: Record<string, string>, body: unknown);
  }

  export interface Request<T = string> {
    requestBody: T;
    url: string;
    params: Record<string, string | number>;
    queryParams: Record<string, string>;
    method: string;
    requestHeaders: Record<string, string>;
  }

  export type NormalizedRequestAttrs<T> = {
    [P in keyof T]: T[P] extends DS.Model & DS.PromiseObject<DS.Model>
      ? never
      : T[P] extends DS.Model
      ? never
      : T[P] extends DS.PromiseManyArray<DS.Model>
      ? never
      : T[P] extends Array<DS.Model> & DS.PromiseManyArray<DS.Model>
      ? never
      : T[P] extends Array<DS.Model>
      ? never
      : T[P];
  };

  export interface HandlerContext {
    request: Request;
    serialize<T = unknown>(modelOrCollection: ModelInstance<T> | ModelClass<T>, serializerName?: string): T;
    serialize<T = unknown>(
      modelOrCollection: DatabaseCollection<T> | Array<ModelInstance<T>> | Collection<T>,
      serializerName?: string
    ): Array<T>;
    normalizedRequestAttrs<M extends keyof ModelRegistry>(model: M): NormalizedRequestAttrs<ModelRegistry[M]>;
  }

  interface HandlerObject {
    [k: string]: unknown;
  }

  interface HandlerOptions {
    timing?: number;
    coalesce?: boolean;
  }

  export type HandlerFunction = (this: HandlerContext, schema: Schema, request: Request) => unknown;

  /* tslint:disable unified-signatures */
  export function handlerDefinition(path: string, options?: HandlerOptions): void;
  export function handlerDefinition(path: string, shorthand: string | Array<string>, options?: HandlerOptions): void;
  export function handlerDefinition(
    path: string,
    shorthand: string | Array<string>,
    responseCode: number,
    options?: HandlerOptions
  ): void;
  export function handlerDefinition(path: string, responseCode?: number, options?: HandlerOptions): void;
  export function handlerDefinition(
    path: string,
    handler: HandlerFunction | HandlerObject,
    options?: HandlerOptions
  ): void;
  export function handlerDefinition(
    path: string,
    handler: HandlerFunction | HandlerObject,
    responseCode: number,
    options?: HandlerOptions
  ): void;
  /* tslint:enable unified-signatures */

  export type ResourceAction = 'index' | 'show' | 'create' | 'update' | 'delete';

  export type ModelAttrs<T> = {
    [P in keyof T]: P extends 'id'
      ? string | number
      : T[P] extends DS.Model & DS.PromiseObject<infer M>
      ? ModelInstance<M>
      : T[P] extends DS.Model
      ? ModelInstance<T[P]>
      : T[P] extends DS.PromiseManyArray<infer M>
      ? Array<ModelInstance<M>>
      : T[P] extends Array<DS.Model> & DS.PromiseManyArray<infer M>
      ? Array<ModelInstance<M>>
      : T[P] extends Array<DS.Model>
      ? Array<ModelInstance<T[P]>>
      : T[P] extends Date
      ? Date | string
      : T[P];
  };

  export type ModelRegistry = EmberDataModelRegistry & IMirageModelRegistry;

  export interface Server {
    schema: Schema;
    db: Database;

    namespace: string;
    timing: number;
    logging: boolean;
    pretender: unknown;
    urlPrefix: string;

    get: typeof handlerDefinition;
    post: typeof handlerDefinition;
    put: typeof handlerDefinition;
    patch: typeof handlerDefinition;
    del: typeof handlerDefinition;

    resource(
      resourceName: string,
      options?: { only?: Array<ResourceAction>; except?: Array<ResourceAction>; path?: string }
    ): void;

    loadFixtures(...fixtures: Array<string>): void;

    passthrough(...args: Array<string | unknown>): void;

    create<T extends keyof ModelRegistry>(modelName: T, ...traits: Array<string>): ModelInstance<ModelRegistry[T]>;
    create<T extends keyof ModelRegistry>(
      modelName: T,
      attrs?: Partial<ModelAttrs<ModelRegistry[T]>>,
      ...traits: Array<string>
    ): ModelInstance<ModelRegistry[T]>;

    createList<T extends keyof ModelRegistry>(
      modelName: T,
      amount: number,
      ...traits: Array<string>
    ): Array<ModelInstance<ModelRegistry[T]>>;
    createList<T extends keyof ModelRegistry>(
      modelName: T,
      amount: number,
      attrs?: Partial<ModelAttrs<ModelRegistry[T]>>,
      ...traits: Array<string>
    ): Array<ModelInstance<ModelRegistry[T]>>;

    serializerOrRegistry: {
      serializerFor(
        modelName: keyof IMirageSchemaRegistry
      ): {
        serialize: HandlerContext['serialize'];
      };
    };

    shutdown(): void;
  }

  export type TraitOptions<M extends ModelRegistry[keyof ModelRegistry]> = Partial<M> & {
    afterCreate?: (obj: ModelInstance<M>, svr: Server) => void;
  };

  export interface Trait<M extends ModelRegistry[keyof ModelRegistry], O extends TraitOptions<M> = {}> {
    extension: O;
    __isTrait__: true;
  }

  export function trait<M extends ModelRegistry[keyof ModelRegistry], O extends TraitOptions<M> = TraitOptions<M>>(
    options: O
  ): Trait<O>;

  // limited by https://github.com/Microsoft/TypeScript/issues/1360
  // function association(...traits: Array<string>, overrides?: { [key: string]: unknown }): unknown;
  export function association(...args: Array<unknown>): unknown;

  export type FactoryAttrs<T> = { [P in keyof T]: T[P] | ((index: number) => T[P]) } & {
    afterCreate?(newObj: ModelInstance<T>, server: Server): void;
  };

  export class FactoryClass<T> {
    static extend<T>(attrs: FactoryAttrs<T>): typeof FactoryClass;
    build(sequence: number): T;
  }

  export const Factory: typeof FactoryClass;

  export class JSONAPISerializer extends EmberObject {
    request: Request;

    keyForAttribute(attr: string): string;
    keyForCollection(modelName: string): string;
    keyForModel(modelName: string): string;
    keyForRelationship(relationship: string): string;
    typeKeyForModel(model: ModelInstance): string;

    serialize(object: ModelInstance, request: Request): Record<string, unknown>;
    normalize(json: unknown): unknown;
  }

  export class Serializer extends EmberObject {}
}
