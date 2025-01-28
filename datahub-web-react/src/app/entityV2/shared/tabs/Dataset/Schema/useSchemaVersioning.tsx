import navigateToUrl from '@app/utils/navigateToUrl';
import { useGetSchemaVersionListQuery } from '@graphql/schemaBlame.generated';
import { useGetVersionedDatasetQuery } from '@graphql/versionedDataset.generated';
import { EditableSchemaMetadata, Schema, SemanticVersionStruct } from '@types';
import * as QueryString from 'query-string';
import { useEffect } from 'react';
import { useHistory, useLocation } from 'react-router-dom';

interface Args {
    datasetUrn?: string;
    urlParam: string;
    skip: boolean;
}

interface Return {
    selectedVersion: string;
    versionList: SemanticVersionStruct[];
    isLatest: boolean;
    schema?: Partial<Schema>;
    editableSchemaMetadata?: EditableSchemaMetadata;
}

export default function useSchemaVersioning({ datasetUrn, urlParam, skip }: Args): Return {
    const location = useLocation();
    const history = useHistory();
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const urlVersion = params[urlParam] as string;

    const { data } = useGetSchemaVersionListQuery({
        skip: !datasetUrn || skip,
        variables: { input: { datasetUrn: datasetUrn || '' } },
        fetchPolicy: 'cache-first',
    });

    const versionList = data?.getSchemaVersionList?.semanticVersionList || [];
    const latestVersion = versionList[0]?.semanticVersion || '';
    const selectedVersion = urlVersion || latestVersion;
    const selectedVersionStruct = versionList.find((v) => v.semanticVersion === selectedVersion);

    useEffect(() => {
        // If the version in the URL is invalid, reset the URL to the latest version
        if (urlVersion && !selectedVersionStruct) {
            navigateToUrl({
                location,
                history,
                urlParam,
                value: '',
            });
        }
    }, [urlVersion, selectedVersionStruct, history, location, urlParam]);

    const versionedDatasetData = useGetVersionedDatasetQuery({
        skip: !datasetUrn || !selectedVersionStruct || skip,
        variables: {
            urn: datasetUrn || '',
            versionStamp: selectedVersionStruct?.versionStamp,
        },
        fetchPolicy: 'cache-first',
    });

    return {
        selectedVersion,
        versionList,
        isLatest: selectedVersion === latestVersion,
        schema: versionedDatasetData.data?.versionedDataset?.schema ?? undefined,
        editableSchemaMetadata: versionedDatasetData.data?.versionedDataset?.editableSchemaMetadata ?? undefined,
    };
}
