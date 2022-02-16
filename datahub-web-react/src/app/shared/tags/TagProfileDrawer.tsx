import React, { useState, useEffect } from 'react';
import { Alert, Drawer, Button, Typography, Divider, Space, message } from 'antd';
import styled from 'styled-components';
import { grey } from '@ant-design/colors';
import { useHistory } from 'react-router';
import { EditOutlined, PlusOutlined } from '@ant-design/icons';
import { ChromePicker } from 'react-color';

import { useGetTagWithColorQuery } from '../../../graphql/tag.generated';
import { useEntityRegistry } from '../../useEntityRegistry';
import { EntityType } from '../../../types.generated';
import { useGetAllEntitySearchResults } from '../../../utils/customGraphQL/useGetAllEntitySearchResults';
import { navigateToSearchUrl } from '../../search/utils/navigateToSearchUrl';
import { useSetTagColorMutation, useUpdateDescriptionMutation } from '../../../graphql/mutations.generated';
import { ExpandedOwner } from '../../entity/shared/components/styled/ExpandedOwner';
import { EMPTY_MESSAGES } from '../../entity/shared/constants';
import { AddOwnerModal } from '../../entity/shared/containers/profile/sidebar/Ownership/AddOwnerModal';
import MarkdownViewer from '../../entity/shared/components/legacy/MarkdownViewer';
import StripMarkdownText, { removeMarkdown } from '../../entity/shared/components/styled/StripMarkdownText';
import EditDescriptionModal from './EditDescriptionModal';
import analytics from '../../analytics/analytics';
import { EntityActionType, EventType } from '../../analytics/event';

type Props = {
    closeTagProfileDrawer?: () => void;
    tagProfileDrawerVisible?: boolean;
    urn: string;
};

const TitleLabel = styled(Typography.Text)`
    &&& {
        color: ${grey[2]};
        font-size: 13px;
        display: block;
    }
`;

const TitleText = styled(Typography.Text)`
    &&& {
        color: rgba(0, 0, 0, 0.85);
        font-weight: 600;
        font-size: 21px;
        line-height: 1.35;
        display: inline-block;
        margin-left: 5px;
    }
`;

const ColorPicker = styled.div`
    position: relative;
    display: inline-block;
    margin-top: 1em;
`;

const ColorPickerButton = styled.div`
    width: 20px;
    height: 20px;
    border: none;
    border-radius: 50%;
`;

const ColorPickerPopOver = styled.div`
    position: absolute;
    z-index: 100;
`;

const NoDescriptionText = styled(Typography.Text)`
    &&& {
        font-size: 13px;
        font-weight: 500;
        font-style: italic;
        display: block;
    }
`;

const DescriptionText = styled(MarkdownViewer)`
    padding-right: 8px;
    display: block;
`;

const EditIcon = styled(EditOutlined)`
    cursor: pointer;
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
        font-size: 13px;
        font-weight: 600;
    }
`;

const EmptyStatsText = styled(Typography.Text)`
    font-size: 15px;
    font-style: italic;
`;

const StatsButton = styled(Button)`
    padding: 5.6px 0px;
    margin-top: -4px;
`;

const ExpandedActions = styled.div`
    height: 10px;
`;

const ReadLessText = styled(Typography.Link)`
    margin-right: 4px;
`;

const ABBREVIATED_LIMIT = 80;

export const TagProfileDrawer = ({ closeTagProfileDrawer, tagProfileDrawerVisible, urn }: Props) => {
    const { loading, error, data, refetch } = useGetTagWithColorQuery({ variables: { urn } });
    const entityRegistry = useEntityRegistry();
    const history = useHistory();
    const [updateDescription] = useUpdateDescriptionMutation();
    const [setTagColorMutation] = useSetTagColorMutation();

    const entityAndSchemaQuery = `tags:"${data?.tag?.properties?.name}" OR fieldTags:"${data?.tag?.properties?.name}" OR editedFieldTags:"${data?.tag?.properties?.name}"`;
    const entityQuery = `tags:"${data?.tag?.properties?.name}"`;
    const ownersEmpty = !data?.tag?.ownership?.owners?.length;
    const description = data?.tag?.properties?.description || '';
    const hexColor = data?.tag?.properties?.colorHex || '';

    const [showAddModal, setShowAddModal] = useState(false);
    const [showEditModal, setShowEditModal] = useState(false);
    const [updatedDescription, setUpdatedDescription] = useState('');
    const overLimit = removeMarkdown(updatedDescription).length > 80;
    const [expanded, setExpanded] = useState(!overLimit);
    const [displayColorPicker, setDisplayColorPicker] = useState(false);
    const [colorValue, setColorValue] = useState('');

    useEffect(() => {
        setUpdatedDescription(description);
    }, [description]);

    useEffect(() => {
        setColorValue(hexColor);
    }, [hexColor]);

    const allSearchResultsByType = useGetAllEntitySearchResults({
        query: entityAndSchemaQuery,
        start: 0,
        count: 1,
        filters: [],
    });

    const statsLoading = Object.keys(allSearchResultsByType).some((type) => {
        return allSearchResultsByType[type].loading;
    });

    const someStats =
        !statsLoading &&
        Object.keys(allSearchResultsByType).some((type) => {
            return allSearchResultsByType[type]?.data?.search.total > 0;
        });

    if (error || (!loading && !error && !data)) {
        return <Alert type="error" message={error?.message || 'Entity failed to load'} />;
    }

    // Handle Color Picker Change
    const closePicker = async () => {
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
        closePicker();
    };

    const handleColorChange = (color: any) => {
        setColorValue(color?.hex);
    };

    const EditButton = (
        <>
            <EditIcon twoToneColor="#52c41a" onClick={() => setShowEditModal(true)} />
        </>
    );

    const onCloseEditModal = () => setShowEditModal(false);

    // Update Description
    const updateDescriptionValue = (value: string) => {
        setUpdatedDescription(value);
        return updateDescription({
            variables: {
                input: {
                    description: value,
                    resourceUrn: urn,
                },
            },
        });
    };

    // Handle save button click on Edit description Modal
    const handleSaveDescription = async (value: string) => {
        message.loading({ content: 'Saving...' });
        try {
            await updateDescriptionValue(value);
            message.destroy();
            message.success({ content: 'Description Updated', duration: 2 });
            analytics.event({
                type: EventType.EntityActionEvent,
                actionType: EntityActionType.UpdateDescription,
                entityType: EntityType.Tag,
                entityUrn: urn,
            });
            onCloseEditModal();
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
            <Drawer
                width={500}
                placement="right"
                closable={false}
                onClose={closeTagProfileDrawer}
                visible={tagProfileDrawerVisible}
                footer={
                    <Space>
                        <Button type="text" onClick={closeTagProfileDrawer}>
                            Close
                        </Button>
                    </Space>
                }
            >
                <>
                    {/* Tag Title */}
                    <div>
                        <TitleLabel>Tag</TitleLabel>
                        <ColorPicker>
                            <ColorPickerButton style={{ backgroundColor: colorValue }} onClick={handlePickerClick} />
                        </ColorPicker>
                        {displayColorPicker && (
                            <ColorPickerPopOver>
                                <ChromePicker color={colorValue} onChange={handleColorChange} />
                            </ColorPickerPopOver>
                        )}
                        <TitleText>{data?.tag?.properties?.name}</TitleText>
                    </div>
                    <Divider />
                    {/* Tag Description */}
                    {description.length === 0 ? (
                        <>
                            <NoDescriptionText>No Description</NoDescriptionText>
                            {EditButton}
                        </>
                    ) : (
                        <>
                            {expanded ? (
                                <>
                                    {updatedDescription && <DescriptionText source={updatedDescription} />}
                                    {updatedDescription && (
                                        <ExpandedActions>
                                            {overLimit && (
                                                <ReadLessText
                                                    onClick={() => {
                                                        setExpanded(false);
                                                    }}
                                                >
                                                    Read Less
                                                </ReadLessText>
                                            )}
                                            {EditButton}
                                        </ExpandedActions>
                                    )}
                                </>
                            ) : (
                                <>
                                    <StripMarkdownText
                                        limit={ABBREVIATED_LIMIT}
                                        readMore={
                                            <>
                                                <Typography.Link
                                                    onClick={() => {
                                                        setExpanded(true);
                                                    }}
                                                >
                                                    Read More
                                                </Typography.Link>
                                            </>
                                        }
                                        suffix={EditButton}
                                    >
                                        {updatedDescription}
                                    </StripMarkdownText>
                                </>
                            )}
                        </>
                    )}
                    {/* Edit Description Modal */}
                    {showEditModal && (
                        <div>
                            <EditDescriptionModal
                                title={updatedDescription ? 'Update description' : 'Add description'}
                                description={updatedDescription}
                                onClose={onCloseEditModal}
                                onSubmit={(value: string) => {
                                    handleSaveDescription(value.trim());
                                }}
                                isAddDesc={!updatedDescription}
                            />
                        </div>
                    )}
                    <Divider />
                    {/* Tag Charts, Datasets and Owners */}
                    <DetailsLayout>
                        <StatsBox>
                            <StatsLabel>Applied to</StatsLabel>
                            {statsLoading && (
                                <div>
                                    <EmptyStatsText>Loading...</EmptyStatsText>
                                </div>
                            )}
                            {!statsLoading && !someStats && (
                                <div>
                                    <EmptyStatsText>No entities</EmptyStatsText>
                                </div>
                            )}
                            {!statsLoading &&
                                someStats &&
                                Object.keys(allSearchResultsByType).map((type) => {
                                    if (allSearchResultsByType[type]?.data?.search.total === 0) {
                                        return null;
                                    }
                                    return (
                                        <div key={type}>
                                            <StatsButton
                                                onClick={() =>
                                                    navigateToSearchUrl({
                                                        type: type as EntityType,
                                                        query:
                                                            type === EntityType.Dataset
                                                                ? entityAndSchemaQuery
                                                                : entityQuery,
                                                        history,
                                                    })
                                                }
                                                type="link"
                                            >
                                                <span data-testid={`stats-${type}`}>
                                                    {allSearchResultsByType[type]?.data?.search.total}{' '}
                                                    {entityRegistry.getCollectionName(type as EntityType)} &gt;
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
                                    <ExpandedOwner entityUrn={urn} owner={owner} refetch={refetch} />
                                ))}
                                {ownersEmpty && (
                                    <Typography.Paragraph type="secondary">
                                        {EMPTY_MESSAGES.owners.title}. {EMPTY_MESSAGES.owners.description}
                                    </Typography.Paragraph>
                                )}
                                <Button type={ownersEmpty ? 'default' : 'text'} onClick={() => setShowAddModal(true)}>
                                    <PlusOutlined /> Add Owner
                                </Button>
                            </div>
                            <div>
                                <AddOwnerModal
                                    visible={showAddModal}
                                    refetch={refetch}
                                    onClose={() => {
                                        setShowAddModal(false);
                                    }}
                                    urn={urn}
                                    entityType={EntityType.Tag}
                                />
                            </div>
                        </div>
                    </DetailsLayout>
                </>
            </Drawer>
        </>
    );
};
