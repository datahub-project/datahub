/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { ActorWithDisplayNameFragment } from '@graphql/query.generated';
import { CorpUser, Entity, SchemaFieldEntity } from '@types';

export type QueryBuilderState = {
    urn?: string;
    query: string;
    title?: string;
    description?: string;
};

export type Query = {
    urn?: string;
    query: string;
    title?: string;
    description?: string;
    lastRun?: number;
    createdTime?: number;
    createdBy?: ActorWithDisplayNameFragment | null;
    poweredEntity?: Entity;
    usedBy?: CorpUser[];
    columns?: SchemaFieldEntity[];
};

export enum QueriesTabSection {
    Highlighted,
    Popular,
    Recent,
    Downstream,
}
