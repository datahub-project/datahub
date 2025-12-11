/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { BrowseResults } from '@types';

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
