import React, { useState, useEffect } from 'react';
import { Alert, Drawer, Button, Space, message } from 'antd';
import { ApolloError } from '@apollo/client';
import styled from 'styled-components';
import { InfoCircleOutlined } from '@ant-design/icons';

import { useGetTagQuery } from '../../../graphql/tag.generated';
import { EntityType, FacetMetadata, Maybe, Scalars } from '../../../types.generated';
import { useSetTagColorMutation, useUpdateDescriptionMutation } from '../../../graphql/mutations.generated';
import analytics from '../../analytics/analytics';
import { EntityActionType, EventType } from '../../analytics/event';
import { SearchResultInterface, GetSearchResultsParams } from '../../entity/shared/components/styled/search/types';
import { useGetSearchResultsForMultipleQuery } from '../../../graphql/search.generated';
import { Message } from '../Message';
import TagStyleEntity from '../TagStyleEntity';

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
    closeTagProfileDrawer?: () => void;
    tagProfileDrawerVisible?: boolean;
    urn: string;
    useGetSearchResults?: (params: GetSearchResultsParams) => {
        data: SearchResultsInterface | undefined | null;
        loading: boolean;
        error: ApolloError | undefined;
    };
};

const DetailsLayout = styled.div`
    display: flex;
    justify-content: space-between;
`;
const LoadingMessage = styled(Message)`
    margin-top: 10%;
`;

export const TagProfileDrawer = ({
    closeTagProfileDrawer,
    tagProfileDrawerVisible,
    urn,
    useGetSearchResults = useWrappedSearchResults,
}: Props) => {
    const { loading, error, data, refetch } = useGetTagQuery({ variables: { urn } });
    const [updateDescription] = useUpdateDescriptionMutation();
    const [setTagColorMutation] = useSetTagColorMutation();
    const entityAndSchemaQuery = `tags:"${data?.tag?.properties?.name}" OR fieldTags:"${data?.tag?.properties?.name}" OR editedFieldTags:"${data?.tag?.properties?.name}"`;
    const entityQuery = `tags:"${data?.tag?.properties?.name}"`;

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
        <>
            <Drawer
                width={500}
                placement="right"
                maskClosable={false}
                closable={false}
                onClose={closeTagProfileDrawer}
                visible={tagProfileDrawerVisible}
                footer={
                    <DetailsLayout>
                        <Space>
                            <Button type="text" onClick={closeTagProfileDrawer}>
                                Close
                            </Button>
                        </Space>
                        <Space>
                            <Button href={`/tag/${urn}`}>
                                <InfoCircleOutlined /> Tag Details
                            </Button>
                        </Space>
                    </DetailsLayout>
                }
            >
                <>
                    {loading && <LoadingMessage type="loading" content="Loading..." />}
                    <TagStyleEntity
                        urn={urn}
                        data={data}
                        refetch={refetch}
                        colorValue={colorValue}
                        handlePickerClick={handlePickerClick}
                        displayColorPicker={displayColorPicker}
                        handleColorChange={handleColorChange}
                        updatedDescription={updatedDescription}
                        handleSaveDescription={handleSaveDescription}
                        facetLoading={facetLoading}
                        aggregations={aggregations}
                        entityAndSchemaQuery={entityAndSchemaQuery}
                        entityQuery={entityQuery}
                        ownersEmpty={ownersEmpty}
                        setShowAddModal={setShowAddModal}
                        showAddModal={showAddModal}
                    />
                </>
            </Drawer>
        </>
    );
};
