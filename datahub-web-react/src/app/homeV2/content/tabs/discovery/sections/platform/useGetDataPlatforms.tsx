/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useGetDataPlatformsQuery } from '@graphql/dataPlatform.generated';
import { DataPlatform } from '@types';

export const DATA_PLATFORMS_URNS = [
    'urn:li:dataPlatform:snowflake',
    'urn:li:dataPlatform:bigquery',
    'urn:li:dataPlatform:redshift',
    'urn:li:dataPlatform:databricks',
    'urn:li:dataPlatform:hive',
    'urn:li:dataPlatform:tableau',
    'urn:li:dataPlatform:looker',
    'urn:li:dataPlatform:powerbi',
    'urn:li:dataPlatform:kafka',
];

export const useGetDataPlatforms = () => {
    const { data } = useGetDataPlatformsQuery({
        variables: {
            urns: DATA_PLATFORMS_URNS,
        },
        fetchPolicy: 'no-cache',
    });

    const platforms =
        data?.entities?.map((content) => ({
            platform: content as DataPlatform,
        })) || [];

    return platforms;
};
