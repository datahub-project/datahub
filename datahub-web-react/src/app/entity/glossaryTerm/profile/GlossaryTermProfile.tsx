import { Alert } from 'antd';
import React, { useMemo } from 'react';
import { GetGlossaryTermQuery, useGetGlossaryTermQuery } from '../../../../graphql/glossaryTerm.generated';
import { EntityType, GlossaryTerm, SearchResult } from '../../../../types.generated';
import { useGetEntitySearchResults } from '../../../../utils/customGraphQL/useGetEntitySearchResults';
import { LegacyEntityProfile } from '../../../shared/LegacyEntityProfile';
import RelatedEntityResults from '../../../shared/entitySearch/RelatedEntityResults';
import useUserParams from '../../../shared/entitySearch/routingUtils/useUserParams';
import { Message } from '../../../shared/Message';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { Properties as PropertiesView } from '../../shared/components/legacy/Properties';
import GlossayRelatedTerms from './GlossaryRelatedTerms';
import GlossaryTermHeader from './GlossaryTermHeader';
import SchemaView from './SchemaView';

const messageStyle = { marginTop: '10%' };

export enum TabType {
    RelatedEntity = 'Related Entities',
    RelatedGlossaryTerms = 'Related Terms',
    Schema = 'Schema',
    Properties = 'Properties',
}

const ENABLED_TAB_TYPES = [TabType.Properties, TabType.RelatedEntity, TabType.RelatedGlossaryTerms, TabType.Schema];

export default function GlossaryTermProfile() {
    const { urn } = useUserParams();
    const { loading, error, data } = useGetGlossaryTermQuery({ variables: { urn } });

    const entityRegistry = useEntityRegistry();
    const searchTypes = entityRegistry.getSearchEntityTypes();
    searchTypes.splice(searchTypes.indexOf(EntityType.GlossaryTerm), 1);

    const glossaryTermHierarchicalName = data?.glossaryTerm?.hierarchicalName;
    const entitySearchResult = useGetEntitySearchResults(
        {
            query: `glossaryTerms:"${glossaryTermHierarchicalName}" OR fieldGlossaryTerms:"${glossaryTermHierarchicalName}" OR editedFieldGlossaryTerms:"${glossaryTermHierarchicalName}"`,
        },
        searchTypes,
    );

    const contentLoading =
        Object.keys(entitySearchResult).some((type) => {
            return entitySearchResult[type].loading;
        }) || loading;

    const entitySearchForDetails = useMemo(() => {
        const filteredSearchResult: {
            [key in EntityType]?: Array<SearchResult>;
        } = {};

        Object.keys(entitySearchResult).forEach((type) => {
            const entities = entitySearchResult[type].data?.search?.searchResults;
            if (entities && entities.length > 0) {
                filteredSearchResult[type] = entities;
            }
        });

        return filteredSearchResult;
    }, [entitySearchResult]);

    const getTabs = ({ glossaryTerm }: GetGlossaryTermQuery) => {
        return [
            {
                name: TabType.RelatedEntity,
                path: TabType.RelatedEntity.toLocaleLowerCase(),
                content: <RelatedEntityResults searchResult={entitySearchForDetails} />,
            },
            {
                name: TabType.RelatedGlossaryTerms,
                path: TabType.RelatedGlossaryTerms.toLocaleLowerCase(),
                content: <GlossayRelatedTerms glossaryTerm={glossaryTerm || {}} />,
            },
            {
                name: TabType.Schema,
                path: TabType.Schema.toLocaleLowerCase(),
                content: <SchemaView rawSchema={glossaryTerm?.glossaryTermInfo?.rawSchema || ''} />,
            },
            {
                name: TabType.Properties,
                path: TabType.Properties.toLocaleLowerCase(),
                content: <PropertiesView properties={glossaryTerm?.glossaryTermInfo?.customProperties || []} />,
            },
        ].filter((tab) => ENABLED_TAB_TYPES.includes(tab.name));
    };

    const getHeader = ({ glossaryTermInfo, ownership }: GlossaryTerm) => {
        return (
            <GlossaryTermHeader
                sourceRef={glossaryTermInfo?.sourceRef || ''}
                sourceUrl={glossaryTermInfo?.sourceUrl as string}
                definition={glossaryTermInfo?.definition as string}
                ownership={ownership}
            />
        );
    };

    if (error || (!loading && !error && !data)) {
        return <Alert type="error" message={error?.message || 'Entity failed to load'} />;
    }

    return (
        <>
            {contentLoading && <Message type="loading" content="Loading..." style={messageStyle} />}
            {data && data.glossaryTerm && (
                <LegacyEntityProfile
                    title={data.glossaryTerm.name}
                    tags={null}
                    header={getHeader(data?.glossaryTerm as GlossaryTerm)}
                    tabs={getTabs(data)}
                />
            )}
        </>
    );
}
