/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { findDatasetByURN } from '@graphql-mock/fixtures/searchResult/datasetSearchResult';
import { Dataset, InstitutionalMemory, InstitutionalMemoryMetadata } from '@types';

type GetDataset = {
    data: { dataset: Dataset };
};

export const getDatasetResolver = {
    getDataset({ variables: { urn } }): GetDataset {
        const dataset = findDatasetByURN(urn);

        if (!dataset.institutionalMemory) {
            const baseElements: InstitutionalMemoryMetadata[] = [];
            const baseInstitutionalMemory: InstitutionalMemory = {
                elements: baseElements,
                __typename: 'InstitutionalMemory',
            };
            dataset.institutionalMemory = baseInstitutionalMemory;
        }

        return {
            data: {
                dataset: Object.assign(dataset, {
                    schema: null,
                    editableProperties: null,
                    editableSchemaMetadata: null,
                    deprecation: null,
                    downstreamLineage: {
                        entities: [],
                        __typename: 'DownstreamEntityRelationships',
                    },
                    upstreamLineage: {
                        entities: [],
                        __typename: 'UpstreamEntityRelationships',
                    },
                    glossaryTerms: null,
                    institutionalMemory: null,
                    usageStats: null,
                }),
            },
        };
    },
};
