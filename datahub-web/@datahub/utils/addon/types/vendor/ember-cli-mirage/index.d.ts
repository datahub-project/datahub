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

  export const faker: Faker.FakerStatic;

  export type MirageID = number | string;

  type MirageRecord<T> = T & { id: MirageID };

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  export interface DatabaseCollection<T = any> {
    insert<S extends T | Array<T>>(data: S): S extends T ? MirageRecord<T> : Array<MirageRecord<T>>;
    find<S extends MirageID | Array<MirageID>>(ids: S): S extends MirageID ? MirageRecord<T> : Array<MirageRecord<T>>;
    findBy(query: T): MirageRecord<T>;
    where(query: T | ((r: MirageRecord<T>) => boolean)): Array<MirageRecord<T>>;
    update(attrs: T): Array<MirageRecord<T>>;
    update(target: MirageID | T, attrs: T): Array<MirageRecord<T>>;
    remove(target?: MirageID | T): void;
    firstOrCreate(query: T, attributesForCreate?: T): MirageRecord<T>;
    readonly length: number;
  }

  export type Database = {
    createCollection: (name: string) => void;
  } & {
    [collectionName: string]: DatabaseCollection;
  };

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

  interface Request<T = unknown> {
    requestBody: T;
    url: string;
    params: {
      [key: string]: string | number;
    };
    queryParams: {
      [key: string]: string;
    };
    method: string;
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
    serialize(modelOrCollection: ModelInstance | Array<ModelInstance> | ModelClass, serializerName?: string): unknown;
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

    // limited by https://github.com/Microsoft/TypeScript/issues/1360
    // passthrough(...paths: Array<string>, verbs?: Array<Verb>): void;
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

    shutdown(): void;
  }

  export type TraitOptions<M> = Record<string, unknown> & {
    afterCreate?: (obj: ModelInstance<M>, svr: Server) => void;
  };

  export interface Trait<O extends TraitOptions<Record<string, unknown>> = {}> {
    extension: O;
    __isTrait__: true;
  }

  export function trait<M extends ModelRegistry[keyof ModelRegistry], O extends TraitOptions<M> = TraitOptions<M>>(
    options: O
  ): Trait<O>;

  // limited by https://github.com/Microsoft/TypeScript/issues/1360
  // function association(...traits: Array<string>, overrides?: { [key: string]: unknown }): unknown;
  export function association(...args: Array<unknown>): unknown;

  export type FactoryAttrs<T> = { [P in keyof T]?: T[P] | ((index: number) => T[P]) } & {
    afterCreate?(newObj: ModelInstance<T>, server: Server): void;
  };

  export class FactoryClass {
    extend<T>(attrs: FactoryAttrs<T>): FactoryClass;
  }

  export const Factory: FactoryClass;

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
