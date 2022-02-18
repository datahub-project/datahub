import React, { useEffect, useState } from 'react';
import { useParams } from 'react-router';
import { Alert, message } from 'antd';
import styled from 'styled-components';
import { ApolloError } from '@apollo/client';

import { useGetTagQuery } from '../../../graphql/tag.generated';
import { EntityType, FacetMetadata, Maybe, Scalars } from '../../../types.generated';
import { Message } from '../../shared/Message';
import { decodeUrn } from '../shared/utils';
import { useGetSearchResultsForMultipleQuery } from '../../../graphql/search.generated';
import { GetSearchResultsParams, SearchResultInterface } from '../shared/components/styled/search/types';
import { useSetTagColorMutation, useUpdateDescriptionMutation } from '../../../graphql/mutations.generated';
import analytics from '../../analytics/analytics';
import { EventType, EntityActionType } from '../../analytics';
import TagStyleEntity from '../../shared/TagStyleEntity';

const PageContainer = styled.div`
    padding: 32px 100px;
`;

const LoadingMessage = styled(Message)`
    margin-top: 10%;
`;

type TagPageParams = {
    urn: string;
};

function useWrappedSearchResults(params: GetSearchResultsParams) {
    const { data, loading, error } = useGetSearchResultsForMultipleQuery(params);
    return { data: data?.searchAcrossEntities, loading, error };
}

type SearchResultsInterface = {
    /** The offset of the result set */
    start: Scalars['Int'];
    /** The number of entities included in the result set */
    count: Scalars['Int'];
    /** The total number of search results matching the query and filters */
    total: Scalars['Int'];
    /** The search result entities */
    searchResults: Array<SearchResultInterface>;
    /** Candidate facet aggregations used for search filtering */
    facets?: Maybe<Array<FacetMetadata>>;
};

type Props = {
    useGetSearchResults?: (params: GetSearchResultsParams) => {
        data: SearchResultsInterface | undefined | null;
        loading: boolean;
        error: ApolloError | undefined;
    };
};

/**
 * Responsible for displaying metadata about a tag
 */
export default function TagProfile({ useGetSearchResults = useWrappedSearchResults }: Props) {
    const { urn: encodedUrn } = useParams<TagPageParams>();
    const urn = decodeUrn(encodedUrn);
    const { loading, error, data, refetch } = useGetTagQuery({ variables: { urn } });
    const [updateDescription] = useUpdateDescriptionMutation();
    const [setTagColorMutation] = useSetTagColorMutation();
    const entityAndSchemaQuery = `tags:"${data?.tag?.name}" OR fieldTags:"${data?.tag?.name}" OR editedFieldTags:"${data?.tag?.name}"`;
    const entityQuery = `tags:"${data?.tag?.name}"`;

    const description = data?.tag?.properties?.description || '';
    const [updatedDescription, setUpdatedDescription] = useState('');
    const hexColor = data?.tag?.properties?.colorHex || '';
    const [displayColorPicker, setDisplayColorPicker] = useState(false);
    const [colorValue, setColorValue] = useState('');
    const ownersEmpty = !data?.tag?.ownership?.owners?.length;
    const [showAddModal, setShowAddModal] = useState(false);

    useEffect(() => {
        setUpdatedDescription(description);
    }, [description]);

    useEffect(() => {
        setColorValue(hexColor);
    }, [hexColor]);

    const { data: facetData, loading: facetLoading } = useGetSearchResults({
        variables: {
            input: {
                query: entityAndSchemaQuery,
                start: 0,
                count: 1,
                filters: [],
            },
        },
    });

    const facets = facetData?.facets?.filter((facet) => facet?.field === 'entity') || [];
    const aggregations = facets && facets[0]?.aggregations;

    // Save Color Change
    const saveColor = async () => {
        if (displayColorPicker) {
            message.loading({ content: 'Saving...' });
            try {
                await setTagColorMutation({
                    variables: {
                        urn,
                        colorHex: colorValue,
                    },
                });
                message.destroy();
                message.success({ content: 'Color Updated', duration: 2 });
                setDisplayColorPicker(false);
            } catch (e: unknown) {
                message.destroy();
                if (e instanceof Error) {
                    message.error({ content: `Failed to update Color: \n ${e.message || ''}`, duration: 2 });
                }
            }
            refetch?.();
        }
    };

    const handlePickerClick = () => {
        setDisplayColorPicker(!displayColorPicker);
        saveColor();
    };

    const handleColorChange = (color: any) => {
        setColorValue(color?.hex);
    };

    // Update Description
    const updateDescriptionValue = (desc: string) => {
        setUpdatedDescription(desc);
        return updateDescription({
            variables: {
                input: {
                    description: desc,
                    resourceUrn: urn,
                },
            },
        });
    };

    // Save the description
    const handleSaveDescription = async (desc: string) => {
        message.loading({ content: 'Saving...' });
        try {
            await updateDescriptionValue(desc);
            message.destroy();
            message.success({ content: 'Description Updated', duration: 2 });
            analytics.event({
                type: EventType.EntityActionEvent,
                actionType: EntityActionType.UpdateDescription,
                entityType: EntityType.Tag,
                entityUrn: urn,
            });
        } catch (e: unknown) {
            message.destroy();
            if (e instanceof Error) {
                message.error({ content: `Failed to update description: \n ${e.message || ''}`, duration: 2 });
            }
        }
        refetch?.();
    };

    if (error || (!loading && !error && !data)) {
        return <Alert type="error" message={error?.message || 'Entity failed to load'} />;
    }

    return (
        <PageContainer>
            {loading && <LoadingMessage type="loading" content="Loading..." />}
            <TagStyleEntity
                urn={urn}
                data={data}
                refetch={refetch}
                colorValue={colorValue}
                handlePickerClick={handlePickerClick}
                displayColorPicker={displayColorPicker}
                handleColorChange={(color: string) => handleColorChange(color)}
                updatedDescription={updatedDescription}
                handleSaveDescription={(desc: string) => handleSaveDescription(desc)}
                facetLoading={facetLoading}
                aggregations={aggregations}
                entityAndSchemaQuery={entityAndSchemaQuery}
                entityQuery={entityQuery}
                ownersEmpty={ownersEmpty}
                setShowAddModal={setShowAddModal}
                showAddModal={showAddModal}
            />
        </PageContainer>
    );
}
