import { useGetDataPlatformsQuery } from '../../../../../../../graphql/dataPlatform.generated';
import { DataPlatform } from '../../../../../../../types.generated';

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
