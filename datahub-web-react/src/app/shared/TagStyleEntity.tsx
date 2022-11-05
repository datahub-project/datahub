import React, { useCallback, useEffect, useRef, useState } from 'react';
import { grey } from '@ant-design/colors';
import { Button, Divider, message, Typography } from 'antd';
import { useHistory } from 'react-router';
import { ApolloError } from '@apollo/client';
import styled from 'styled-components';
import { ChromePicker } from 'react-color';
import ColorHash from 'color-hash';
import { PlusOutlined } from '@ant-design/icons';
import { useGetTagQuery } from '../../graphql/tag.generated';
import { EntityType, FacetMetadata, Maybe, Scalars } from '../../types.generated';
import { ExpandedOwner } from '../entity/shared/components/styled/ExpandedOwner';
import { EMPTY_MESSAGES } from '../entity/shared/constants';
import { navigateToSearchUrl } from '../search/utils/navigateToSearchUrl';
import { useEntityRegistry } from '../useEntityRegistry';
import { useUpdateDescriptionMutation, useSetTagColorMutation } from '../../graphql/mutations.generated';
import { useGetSearchResultsForMultipleQuery } from '../../graphql/search.generated';
import analytics, { EventType, EntityActionType } from '../analytics';
import { GetSearchResultsParams, SearchResultInterface } from '../entity/shared/components/styled/search/types';
import { EditOwnersModal } from '../entity/shared/containers/profile/sidebar/Ownership/EditOwnersModal';
import CopyUrn from './CopyUrn';
import EntityDropdown from '../entity/shared/EntityDropdown';
import { EntityMenuItems } from '../entity/shared/EntityDropdown/EntityDropdown';
import { ErrorSection } from './error/ErrorSection';
import { generateOrFilters } from '../search/utils/generateOrFilters';
import { UnionType } from '../search/utils/constants';

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

const TitleLabel = styled(Typography.Text)`
    &&& {
        color: ${grey[2]};
        font-size: 12px;
        display: block;
        line-height: 20px;
        font-weight: 700;
    }
`;

const TitleText = styled(Typography.Text)`
    &&& {
        color: ${grey[10]};
        font-weight: 700;
        font-size: 20px;
        line-height: 28px;
        display: inline-block;
        margin: 0px 7px;
    }
`;

const ColorPicker = styled.div`
    position: relative;
    display: inline-block;
`;

const ColorPickerButton = styled.div`
    width: 16px;
    height: 16px;
    border: none;
    border-radius: 50%;
`;

const ColorPickerPopOver = styled.div`
    position: absolute;
    z-index: 100;
`;

const DescriptionLabel = styled(Typography.Text)`
    &&& {
        text-align: left;
        font-weight: bold;
        font-size: 14px;
        line-height: 28px;
        color: rgb(38, 38, 38);
    }
`;

export const EmptyValue = styled.div`
    &:after {
        content: 'None';
        color: #b7b7b7;
        font-style: italic;
        font-weight: 100;
    }
`;

const DetailsLayout = styled.div`
    display: flex;
    justify-content: space-between;
`;

const StatsBox = styled.div`
    width: 180px;
    justify-content: left;
`;

const StatsLabel = styled(Typography.Text)`
    &&& {
        color: ${grey[10]};
        font-size: 14px;
        font-weight: 700;
        line-height: 28px;
    }
`;

const StatsButton = styled(Button)`
    padding: 0px 0px;
    margin-top: 0px;
    font-weight: 700;
    font-size: 12px;
    line-height: 20px;
`;

const EmptyStatsText = styled(Typography.Text)`
    font-size: 15px;
    font-style: italic;
`;

const OwnerButtonEmptyTitle = styled.span`
    font-weight: 700;
    font-size: 12px;
    line-height: 20px;
    color: ${grey[10]};
`;

const OwnerButtonTitle = styled.span`
    font-weight: 500;
    font-size: 12px;
    line-height: 20px;
    color: ${grey[10]};
`;

const TagName = styled.div`
    display: flex;
    align-items: center;
    justify-content: left;
`;

const ActionButtons = styled.div`
    display: flex;
`;

const TagHeader = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: top;
`;

const { Paragraph } = Typography;

type Props = {
    urn: string;
    useGetSearchResults?: (params: GetSearchResultsParams) => {
        data: SearchResultsInterface | undefined | null;
        loading: boolean;
        error: ApolloError | undefined;
    };
};

const generateColor = new ColorHash({
    saturation: 0.9,
});

/**
 * Responsible for displaying metadata about a tag
 */
export default function TagStyleEntity({ urn, useGetSearchResults = useWrappedSearchResults }: Props) {
    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    const { error, data, refetch } = useGetTagQuery({ variables: { urn } });
    const [updateDescription] = useUpdateDescriptionMutation();
    const [setTagColorMutation] = useSetTagColorMutation();
    const entityUrn = data?.tag?.urn;
    const entityFilters =
        (entityUrn && [
            {
                field: 'tags',
                values: [entityUrn],
            },
        ]) ||
        [];
    const entityAndSchemaFilters =
        (entityUrn && [
            ...entityFilters,
            {
                field: 'fieldTags',
                values: [entityUrn],
            },
        ]) ||
        [];

    const description = data?.tag?.properties?.description || '';
    const [updatedDescription, setUpdatedDescription] = useState('');
    const hexColor = data?.tag?.properties?.colorHex || generateColor.hex(urn);
    const [displayColorPicker, setDisplayColorPicker] = useState(false);
    const [colorValue, setColorValue] = useState('');
    const ownersEmpty = !data?.tag?.ownership?.owners?.length;
    const [showAddModal, setShowAddModal] = useState(false);
    const [copiedUrn, setCopiedUrn] = useState(false);

    useEffect(() => {
        setUpdatedDescription(description);
    }, [description]);

    useEffect(() => {
        setColorValue(hexColor);
    }, [hexColor]);

    const { data: facetData, loading: facetLoading } = useGetSearchResults({
        variables: {
            input: {
                query: '*',
                start: 0,
                count: 1,
                orFilters: generateOrFilters(UnionType.OR, entityAndSchemaFilters),
            },
        },
    });

    const facets = facetData?.facets?.filter((facet) => facet?.field === 'entity') || [];
    const aggregations = facets && facets[0]?.aggregations;

    // Save Color Change
    const saveColor = useCallback(async () => {
        if (displayColorPicker) {
            try {
                await setTagColorMutation({
                    variables: {
                        urn,
                        colorHex: colorValue,
                    },
                });
                message.destroy();
                message.success({ content: 'Color Saved!', duration: 2 });
                setDisplayColorPicker(false);
            } catch (e: unknown) {
                message.destroy();
                if (e instanceof Error) {
                    message.error({ content: `Failed to save tag color: \n ${e.message || ''}`, duration: 2 });
                }
            }
            refetch?.();
        }
    }, [urn, colorValue, displayColorPicker, setTagColorMutation, setDisplayColorPicker, refetch]);

    const colorPickerRef = useRef(null);

    useEffect(() => {
        /**
         * Save Color if Clicked outside of the Color Picker
         */
        function handleClickOutsideColorPicker(event) {
            if (displayColorPicker) {
                const { current }: any = colorPickerRef;
                if (current) {
                    if (!current.contains(event.target)) {
                        setDisplayColorPicker(false);
                        saveColor();
                    }
                }
            }
        }
        // Bind the event listener
        document.addEventListener('mousedown', handleClickOutsideColorPicker);
        return () => {
            // Unbind the event listener on clean up
            document.removeEventListener('mousedown', handleClickOutsideColorPicker);
        };
    }, [colorPickerRef, displayColorPicker, saveColor]);

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

    return (
        <>
            {error && <ErrorSection />}
            {/* Tag Title */}
            <TagHeader>
                <div>
                    <TitleLabel>Tag</TitleLabel>
                    <TagName>
                        <ColorPicker>
                            <ColorPickerButton style={{ backgroundColor: colorValue }} onClick={handlePickerClick} />
                        </ColorPicker>
                        <TitleText>
                            {(data?.tag && entityRegistry.getDisplayName(EntityType.Tag, data?.tag)) || ''}
                        </TitleText>
                    </TagName>
                </div>
                <ActionButtons>
                    <CopyUrn urn={urn} isActive={copiedUrn} onClick={() => setCopiedUrn(true)} />
                    <EntityDropdown
                        urn={urn}
                        entityType={EntityType.Tag}
                        entityData={data?.tag}
                        menuItems={new Set([EntityMenuItems.COPY_URL, EntityMenuItems.DELETE])}
                    />
                </ActionButtons>
                {displayColorPicker && (
                    <ColorPickerPopOver ref={colorPickerRef}>
                        <ChromePicker color={colorValue} onChange={handleColorChange} />
                    </ColorPickerPopOver>
                )}
            </TagHeader>
            <Divider />
            {/* Tag Description */}
            <DescriptionLabel>About</DescriptionLabel>
            <Paragraph
                style={{ fontSize: '12px', lineHeight: '15px', padding: '5px 0px' }}
                editable={{ onChange: handleSaveDescription }}
                ellipsis={{ rows: 2, expandable: true, symbol: 'Read more' }}
            >
                {updatedDescription || <EmptyValue />}
            </Paragraph>
            <Divider />
            {/* Tag Charts, Datasets and Owners */}
            <DetailsLayout>
                <StatsBox>
                    <StatsLabel>Applied to</StatsLabel>
                    {facetLoading && (
                        <div>
                            <EmptyStatsText>Loading...</EmptyStatsText>
                        </div>
                    )}
                    {!facetLoading && aggregations && aggregations?.length === 0 && (
                        <div>
                            <EmptyStatsText>No entities</EmptyStatsText>
                        </div>
                    )}
                    {!facetLoading &&
                        aggregations &&
                        aggregations?.map((aggregation) => {
                            if (aggregation?.count === 0) {
                                return null;
                            }
                            return (
                                <div key={aggregation?.value}>
                                    <StatsButton
                                        onClick={() =>
                                            navigateToSearchUrl({
                                                type: aggregation?.value as EntityType,
                                                filters:
                                                    aggregation?.value === EntityType.Dataset
                                                        ? entityAndSchemaFilters
                                                        : entityFilters,
                                                unionType: UnionType.OR,
                                                history,
                                            })
                                        }
                                        type="link"
                                    >
                                        <span data-testid={`stats-${aggregation?.value}`}>
                                            {aggregation?.count}{' '}
                                            {entityRegistry.getCollectionName(aggregation?.value as EntityType)} &gt;
                                        </span>
                                    </StatsButton>
                                </div>
                            );
                        })}
                </StatsBox>
                <div>
                    <StatsLabel>Owners</StatsLabel>
                    <div>
                        {data?.tag?.ownership?.owners?.map((owner) => (
                            <ExpandedOwner entityUrn={urn} owner={owner} refetch={refetch} hidePopOver />
                        ))}
                        {ownersEmpty && (
                            <Typography.Paragraph type="secondary">
                                {EMPTY_MESSAGES.owners.title}. {EMPTY_MESSAGES.owners.description}
                            </Typography.Paragraph>
                        )}
                        <Button type={ownersEmpty ? 'default' : 'text'} onClick={() => setShowAddModal(true)}>
                            <PlusOutlined />
                            {ownersEmpty ? (
                                <OwnerButtonEmptyTitle>Add Owners</OwnerButtonEmptyTitle>
                            ) : (
                                <OwnerButtonTitle>Add Owners</OwnerButtonTitle>
                            )}
                        </Button>
                    </div>
                    <div>
                        {showAddModal && (
                            <EditOwnersModal
                                hideOwnerType
                                refetch={refetch}
                                onCloseModal={() => {
                                    setShowAddModal(false);
                                }}
                                urns={[urn]}
                                entityType={EntityType.Tag}
                            />
                        )}
                    </div>
                </div>
            </DetailsLayout>
        </>
    );
}
