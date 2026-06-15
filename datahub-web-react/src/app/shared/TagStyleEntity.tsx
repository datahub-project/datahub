import { grey } from '@ant-design/colors';
import { PlusOutlined } from '@ant-design/icons';
import { ApolloError } from '@apollo/client';
import { Button, Divider, Typography, message } from 'antd';
import ColorHash from 'color-hash';
import React, { useCallback, useEffect, useRef, useState } from 'react';
import { ChromePicker } from 'react-color';
import { useTranslation } from 'react-i18next';
import { useHistory } from 'react-router';
import styled from 'styled-components';

import analytics, { EntityActionType, EventType } from '@app/analytics';
import EntityDropdown from '@app/entity/shared/EntityDropdown';
import { EntityMenuItems } from '@app/entity/shared/EntityDropdown/EntityDropdown';
import { ExpandedOwner } from '@app/entity/shared/components/styled/ExpandedOwner/ExpandedOwner';
import { GetSearchResultsParams, SearchResultInterface } from '@app/entity/shared/components/styled/search/types';
import { EMPTY_MESSAGES } from '@app/entity/shared/constants';
import { EditOwnersModal } from '@app/entity/shared/containers/profile/sidebar/Ownership/EditOwnersModal';
import { ENTITY_FILTER_NAME, UnionType } from '@app/search/utils/constants';
import { generateOrFilters } from '@app/search/utils/generateOrFilters';
import { navigateToSearchUrl } from '@app/search/utils/navigateToSearchUrl';
import { DeprecationIcon } from '@app/entityV2/shared/components/styled/DeprecationIcon';
import CopyUrn from '@app/shared/CopyUrn';
import { ErrorSection } from '@app/shared/error/ErrorSection';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useSetTagColorMutation, useUpdateDescriptionMutation } from '@graphql/mutations.generated';
import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { useGetTagQuery } from '@graphql/tag.generated';
import { EntityType, FacetMetadata, Maybe, Scalars } from '@types';

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
        color: ${(props) => props.theme.colors.text};
    }
`;

const EmptyValue = styled.span`
    color: ${(props) => props.theme.colors.textTertiary};
    font-style: italic;
    font-weight: 100;
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
    hideDeleteAction?: boolean;
};

const generateColor = new ColorHash({
    saturation: 0.9,
});

/**
 * Responsible for displaying metadata about a tag
 */
export default function TagStyleEntity({
    urn,
    useGetSearchResults = useWrappedSearchResults,
    hideDeleteAction = false,
}: Props) {
    const { t } = useTranslation('shared.tags');
    const { t: tcFeedback } = useTranslation('common.feedback');
    const { t: tcActions } = useTranslation('common.actions');
    const { t: tcLabels } = useTranslation('common.labels');
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
                orFilters: generateOrFilters(UnionType.OR, entityFilters),
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
                message.success({ content: t('colorSaved'), duration: 2 });
                setDisplayColorPicker(false);
            } catch (e: unknown) {
                message.destroy();
                if (e instanceof Error) {
                    message.error({ content: t('saveColorError', { error: e.message || '' }), duration: 2 });
                }
            }
            refetch?.();
        }
    }, [urn, colorValue, displayColorPicker, setTagColorMutation, setDisplayColorPicker, refetch, t]);

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
        message.loading({ content: tcFeedback('saving') });
        try {
            await updateDescriptionValue(desc);
            message.destroy();
            message.success({ content: t('descriptionUpdated'), duration: 2 });
            analytics.event({
                type: EventType.EntityActionEvent,
                actionType: EntityActionType.UpdateDescription,
                entityType: EntityType.Tag,
                entityUrn: urn,
            });
        } catch (e: unknown) {
            message.destroy();
            if (e instanceof Error) {
                message.error({ content: t('updateDescriptionError', { error: e.message || '' }), duration: 2 });
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
                    <TitleLabel>{t('tagLabel')}</TitleLabel>
                    <TagName>
                        <ColorPicker>
                            <ColorPickerButton style={{ backgroundColor: colorValue }} onClick={handlePickerClick} />
                        </ColorPicker>
                        <TitleText>
                            {(data?.tag && entityRegistry.getDisplayName(EntityType.Tag, data?.tag)) || ''}
                        </TitleText>
                        {data?.tag?.deprecation?.deprecated && (
                            <DeprecationIcon
                                urn={urn}
                                deprecation={data.tag.deprecation}
                                showUndeprecate
                                refetch={refetch}
                                showText={false}
                            />
                        )}
                    </TagName>
                </div>
                <ActionButtons>
                    <CopyUrn urn={urn} isActive={copiedUrn} onClick={() => setCopiedUrn(true)} />
                    {!hideDeleteAction && (
                        <EntityDropdown
                            urn={urn}
                            entityType={EntityType.Tag}
                            entityData={data?.tag}
                            menuItems={new Set([EntityMenuItems.UPDATE_DEPRECATION, EntityMenuItems.DELETE])}
                        />
                    )}
                </ActionButtons>
                {displayColorPicker && (
                    <ColorPickerPopOver ref={colorPickerRef}>
                        <ChromePicker color={colorValue} onChange={handleColorChange} />
                    </ColorPickerPopOver>
                )}
            </TagHeader>
            <Divider />
            {/* Tag Description */}
            <DescriptionLabel>{t('about')}</DescriptionLabel>
            <Paragraph
                style={{ fontSize: '12px', lineHeight: '15px', padding: '5px 0px' }}
                editable={{ onChange: handleSaveDescription }}
                ellipsis={{ rows: 2, expandable: true, symbol: tcActions('readMore') }}
            >
                {updatedDescription || <EmptyValue>{tcLabels('none')}</EmptyValue>}
            </Paragraph>
            <Divider />
            {/* Tag Charts, Datasets and Owners */}
            <DetailsLayout>
                <StatsBox>
                    <StatsLabel>{t('appliedTo')}</StatsLabel>
                    {facetLoading && (
                        <div>
                            <EmptyStatsText>{tcFeedback('loading')}</EmptyStatsText>
                        </div>
                    )}
                    {!facetLoading && aggregations && aggregations?.length === 0 && (
                        <div>
                            <EmptyStatsText>{t('noEntities')}</EmptyStatsText>
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
                                                filters: [
                                                    ...entityFilters,
                                                    {
                                                        field: ENTITY_FILTER_NAME,
                                                        values: [aggregation.value],
                                                    },
                                                ],
                                                unionType: UnionType.AND,
                                                history,
                                            })
                                        }
                                        type="link"
                                    >
                                        <span data-testid={`stats-${aggregation?.value}`}>
                                            {aggregation?.count}{' '}
                                            {/* eslint-disable-next-line i18next/no-literal-string -- (untranslated-text) interpolated entity collection name from registry; only the ">" separator is literal */}
                                            {entityRegistry.getCollectionName(aggregation?.value as EntityType)} &gt;
                                        </span>
                                    </StatsButton>
                                </div>
                            );
                        })}
                </StatsBox>
                <div>
                    <StatsLabel>{tcLabels('owners')}</StatsLabel>
                    <div>
                        {data?.tag?.ownership?.owners?.map((owner) => (
                            <ExpandedOwner entityUrn={urn} owner={owner} refetch={refetch} hidePopOver />
                        ))}
                        {ownersEmpty && (
                            <Typography.Paragraph type="secondary">
                                {/* eslint-disable-next-line i18next/no-literal-string -- (untranslated-text) EMPTY_MESSAGES content from shared constants; only punctuation separator is literal */}
                                {EMPTY_MESSAGES.owners.title}. {EMPTY_MESSAGES.owners.description}
                            </Typography.Paragraph>
                        )}
                        <Button type={ownersEmpty ? 'default' : 'text'} onClick={() => setShowAddModal(true)}>
                            <PlusOutlined />
                            {ownersEmpty ? (
                                <OwnerButtonEmptyTitle>{t('addOwners')}</OwnerButtonEmptyTitle>
                            ) : (
                                <OwnerButtonTitle>{t('addOwners')}</OwnerButtonTitle>
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
