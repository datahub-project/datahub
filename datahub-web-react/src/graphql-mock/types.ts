import { BrowseResults } from '../types.generated';

export type DataSchema = {
    id?: string | undefined;
    attrs: AnyRecord;
    modelName: string;
    save(): void;
    update<K extends never>(key: K, value: AnyRecord[K]): void;
    update(changes: Partial<AnyRecord>): void;
    destroy(): void;
    reload(): void;
};

export type AnyRecord = Record<string, unknown>;

export type EntityBrowsePath = {
    name: string;
    paths: EntityBrowsePath[];
    count?: number;
};

export type StringNumber = string | number;

export type GetBrowseResults = {
    data: {
        browse: BrowseResults;
    };
};

type EntityBrowseFnArg = {
    start: number;
    count: number;
    path: string[];
};

export type EntityBrowseFn = (arg: EntityBrowseFnArg) => GetBrowseResults;
