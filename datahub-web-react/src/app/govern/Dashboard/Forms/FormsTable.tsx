import { NetworkStatus } from '@apollo/client';
import { colors, Icon, Pill, Table, Text, Tooltip, typography } from '@components';
import { AlignmentOptions, ColorOptions } from '@src/alchemy-components/theme/config';
import analytics, { EventType } from '@src/app/analytics';
import { useUserContext } from '@src/app/context/useUserContext';
import { HoverEntityTooltip } from '@src/app/recommendations/renderer/component/HoverEntityTooltip';
import { CustomAvatar } from '@src/app/shared/avatar';
import { capitalizeFirstLetter } from '@src/app/shared/textUtil';
import { toLocalDateString, toRelativeTimeString } from '@src/app/shared/time/timeUtils';
import { ConfirmationModal } from '@src/app/sharedV2/modals/ConfirmationModal';
import { showToastMessage, ToastType } from '@src/app/sharedV2/toastMessageUtils';
import { useAppConfig } from '@src/app/useAppConfig';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { PageRoutes } from '@src/conf/Global';
import { useFormAnalyticsQuery } from '@src/graphql/analytics.generated';
import { useDeleteFormMutation, useUpdateFormMutation } from '@src/graphql/form.generated';
import { GetSearchResultsForMultipleQuery } from '@src/graphql/search.generated';
import { Entity, EntityType, FormState, SearchAcrossEntitiesInput } from '@src/types.generated';
import { Dropdown, Typography } from 'antd';
import React, { useState } from 'react';
import Highlight from 'react-highlighter';
import { useHistory } from 'react-router';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import EmptyForms from './EmptyForms';
import { UNPUBLISH_MODAL_TEXT } from './formUtils';
import { CardIcons } from './styledComponents';

const FormName = styled(Typography.Text)`
    font-size: 14px;
    font-weight: 600;
    color: ${colors.gray[600]};
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;

    &:hover {
        text-decoration: underline;
    }
`;

const FormDescription = styled(Typography.Text)`
    font-size: 14px;
    font-weight: 400;
    color: ${colors.gray[1700]};
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

const CellContainer = styled.div`
    display: flex;
    flex-direction: column;
`;

const StatusContainer = styled.div`
    display: inherit;
`;

const MenuItem = styled.div`
    display: flex;
    padding: 5px 70px 5px 5px;
    font-size: 14px;
    font-weight: 400;
    color: ${colors.gray[600]};
    font-family: ${typography.fonts.body};
`;

export const EditedByContainer = styled.div`
    display: inline-flex;
    align-items: center;
    gap: 4px;
    padding: 3px 6px 3px 4px;
    border-radius: 20px;
    border: 1px solid ${colors.gray[1400]};
    :hover {
        cursor: pointer;
    }
`;

interface Props {
    searchQuery: string;
    searchData: GetSearchResultsForMultipleQuery | undefined;
    loading: boolean;
    networkStatus: NetworkStatus;
    refetch: () => Promise<any>;
    inputs: SearchAcrossEntitiesInput;
}

const FormsTable = ({ searchQuery, searchData, loading, networkStatus, refetch, inputs }: Props) => {
    const entityRegistry = useEntityRegistryV2();
    const history = useHistory();
    const me = useUserContext();
    const canEditForms = me.platformPrivileges?.manageDocumentationForms;
    const { config } = useAppConfig();
    const { showFormAnalytics } = config.featureFlags;
    const [showConfirmDelete, setShowConfirmDelete] = useState<boolean>(false);
    const [showConfirmUnpublish, setShowConfirmUnpublish] = useState<boolean>(false);

    const [currentForm, setCurrentForm] = useState();

    const [deleteForm] = useDeleteFormMutation();
    const [updateForm] = useUpdateFormMutation();

    const formsData = searchData?.searchAcrossEntities?.searchResults || [];

    // Filter the table data based on the search query and sort by published most recently
    const filteredForms = formsData
        .filter((prop: any) => prop.entity.formInfo.name?.toLowerCase().includes(searchQuery.toLowerCase()))
        .sort((propA, propB) => {
            const timeA =
                ((propA.entity as any).formInfo.status.state === FormState.Published &&
                    (propA.entity as any).formInfo.status?.lastModified?.time) ||
                0;
            const timeB =
                ((propB.entity as any).formInfo.status.state === FormState.Published &&
                    (propB.entity as any).formInfo.status?.lastModified?.time) ||
                0;

            return timeB - timeA;
        });

    const isLoading = loading || networkStatus === NetworkStatus.refetch;

    // Fetch max snapshot date
    const { data: snapshot, loading: snapshotLoading } = useFormAnalyticsQuery({
        variables: { input: { queryString: `select max(snapshot_date) from '{{ table }}'` } },
    });

    if (!isLoading && !filteredForms.length) {
        return <EmptyForms isEmptySearch={!!formsData.length} />;
    }

    const handleDeleteForm = (formData) => {
        showToastMessage(ToastType.LOADING, 'Deleting form', 1);
        deleteForm({
            variables: {
                input: {
                    urn: formData.entity.urn,
                },
            },
        })
            .then(() => {
                analytics.event({
                    type: EventType.DeleteFormEvent,
                    formUrn: formData.entity.urn,
                    formType: formData.entity.formInfo?.type,
                    noOfQuestions: formData.entity.formInfo?.prompts?.length,
                    areOwnersAssigned: !!formData.entity.formInfo?.actors?.owners,
                });
                showToastMessage(ToastType.SUCCESS, 'Form deleted successfully!', 3);
                refetch();
            })
            .catch(() => {
                showToastMessage(ToastType.ERROR, 'Failed to delete form', 3);
            })
            .finally(() => {
                setShowConfirmDelete(false);
                setCurrentForm(undefined);
            });
    };

    const handleDeleteClose = () => {
        setShowConfirmDelete(false);
        setCurrentForm(undefined);
    };

    const handleUnpublishForm = (formData) => {
        showToastMessage(ToastType.LOADING, 'Unpublishing form', 1);
        updateForm({
            variables: {
                input: {
                    urn: formData.entity.urn,
                    state: FormState.Unpublished,
                },
            },
        })
            .then(() => {
                showToastMessage(ToastType.SUCCESS, 'Form unpublished successfully!', 3);
                refetch();
            })
            .catch(() => {
                showToastMessage(ToastType.ERROR, 'Failed to unpublished form', 3);
            })
            .finally(() => {
                setShowConfirmUnpublish(false);
                setCurrentForm(undefined);
            });
    };

    const handleUnpublishClose = () => {
        setShowConfirmUnpublish(false);
        setCurrentForm(undefined);
    };

    const formColumns = [
        {
            title: 'Name',
            key: 'name',
            render: (record) => {
                return (
                    <CellContainer>
                        <Link
                            to={{
                                pathname: `/govern/dashboard/edit-form/${record.entity.urn}`,
                                state: {
                                    inputs,
                                    searchAcrossEntities: searchData?.searchAcrossEntities,
                                },
                            }}
                        >
                            <FormName>
                                <Highlight search={searchQuery} data-testid={`${record.entity.urn}-name`}>
                                    {record.entity.formInfo.name}
                                </Highlight>
                            </FormName>
                        </Link>
                        <FormDescription data-testid={`${record.entity.urn}-description`}>
                            {record.entity.formInfo.description}
                        </FormDescription>
                    </CellContainer>
                );
            },
            width: '520px',
            sorter: (sourceA, sourceB) => {
                return sourceA.entity.formInfo.name.localeCompare(sourceB.entity.formInfo.name);
            },
        },
        {
            title: 'Status',
            key: 'status',
            render: (record) => {
                const status = record.entity.formInfo.status?.state;
                let colorScheme: ColorOptions = 'gray';
                if (status === FormState.Published) colorScheme = 'violet';
                else if (status === FormState.Unpublished) colorScheme = 'blue';
                return (
                    <StatusContainer data-testid={`${record.entity.urn}-status-${status.toLowerCase()}`}>
                        <Pill
                            label={status.charAt(0) + status.slice(1).toLowerCase()}
                            color={colorScheme}
                            clickable={false}
                        />
                    </StatusContainer>
                );
            },
            sorter: (sourceA, sourceB) => {
                return sourceA.entity.formInfo.status.state.localeCompare(sourceB.entity.formInfo.status.state);
            },
        },
        {
            title: 'Published',
            key: 'published',
            render: (record) => {
                const publishedTime =
                    record.entity.formInfo.status.state === FormState.Published
                        ? record.entity.formInfo.status?.lastModified?.time
                        : undefined;

                return (
                    <CellContainer>
                        <Tooltip title={publishedTime && toLocalDateString(publishedTime)} showArrow={false}>
                            {publishedTime ? capitalizeFirstLetter(toRelativeTimeString(publishedTime)) : '-'}
                        </Tooltip>
                    </CellContainer>
                );
            },
            sorter: (sourceA, sourceB) => {
                const timeA =
                    sourceA.entity.formInfo.status.state === FormState.Published &&
                    !!sourceA.entity.formInfo.status.lastModified?.time
                        ? sourceA.entity.formInfo.status.lastModified.time
                        : Number.MAX_SAFE_INTEGER;
                const timeB =
                    sourceB.entity.formInfo.status.state === FormState.Published &&
                    !!sourceB.entity.formInfo.status.lastModified?.time
                        ? sourceB.entity.formInfo.status.lastModified.time
                        : Number.MAX_SAFE_INTEGER;

                return timeA - timeB;
            },
        },
        {
            title: 'Last Edited By',
            key: 'lastEditedBy',
            render: (record) => {
                const lastEditedByUser = record.entity.formInfo.lastModified?.actor;
                const name = lastEditedByUser && entityRegistry.getDisplayName(EntityType.CorpUser, lastEditedByUser);
                const avatarUrl = lastEditedByUser?.editableProperties?.pictureLink || undefined;

                return (
                    <>
                        {lastEditedByUser && (
                            <HoverEntityTooltip entity={lastEditedByUser as Entity} showArrow={false}>
                                <Link
                                    to={`${entityRegistry.getEntityUrl(
                                        EntityType.CorpUser,
                                        (lastEditedByUser as Entity).urn,
                                    )}`}
                                >
                                    <EditedByContainer>
                                        <CustomAvatar size={20} name={name} photoUrl={avatarUrl} hideTooltip />
                                        <Text color="gray" size="sm">
                                            {name}
                                        </Text>
                                    </EditedByContainer>
                                </Link>
                            </HoverEntityTooltip>
                        )}
                    </>
                );
            },
            sorter: (sourceA, sourceB) => {
                const lastEditedByUserA = sourceA.entity.formInfo.lastModified?.actor;
                const nameA =
                    lastEditedByUserA && entityRegistry.getDisplayName(EntityType.CorpUser, lastEditedByUserA);
                const lastEditedByUserB = sourceB.entity.formInfo.lastModified?.actor;
                const nameB =
                    lastEditedByUserB && entityRegistry.getDisplayName(EntityType.CorpUser, lastEditedByUserB);

                return nameA?.localeCompare(nameB);
            },
        },
        {
            title: '',
            key: 'actions',
            alignment: 'right' as AlignmentOptions,
            render: (record) => {
                const items = [
                    {
                        key: '0',
                        label: (
                            <MenuItem
                                onClick={() => {
                                    history.push(`/govern/dashboard/edit-form/${record.entity.urn}`, {
                                        inputs,
                                        searchAcrossEntities: searchData?.searchAcrossEntities,
                                    });
                                }}
                                data-testid="action-edit"
                            >
                                Edit
                            </MenuItem>
                        ),
                    },
                    {
                        key: '1',
                        disabled: !canEditForms,
                        label: (
                            <Tooltip
                                showArrow={false}
                                title={
                                    !canEditForms
                                        ? 'Must have permission to manage forms. Ask your DataHub administrator.'
                                        : null
                                }
                            >
                                <MenuItem
                                    onClick={() => {
                                        if (canEditForms) {
                                            setCurrentForm(record);
                                            setShowConfirmDelete(true);
                                        }
                                    }}
                                    data-testid="action-delete"
                                >
                                    <Text color="red">Delete</Text>
                                </MenuItem>
                            </Tooltip>
                        ),
                    },
                ];

                if (record.entity.formInfo.status?.state === FormState.Published) {
                    items.splice(1, 0, {
                        key: '2',
                        disabled: !canEditForms,
                        label: (
                            <Tooltip
                                showArrow={false}
                                title={
                                    !canEditForms
                                        ? 'Must have permission to manage forms. Ask your DataHub administrator.'
                                        : null
                                }
                            >
                                <MenuItem
                                    onClick={() => {
                                        if (canEditForms) {
                                            setCurrentForm(record);
                                            setShowConfirmUnpublish(true);
                                        }
                                    }}
                                    data-testid="action-unpublish"
                                >
                                    Unpublish
                                </MenuItem>
                            </Tooltip>
                        ),
                    });
                }

                // Show Analytics button if form analytics are available
                const areFormAnalyticsAvailable =
                    showFormAnalytics &&
                    !snapshotLoading &&
                    snapshot?.formAnalytics?.errors !== null &&
                    snapshot?.formAnalytics?.header !== null;

                return (
                    <>
                        <CardIcons>
                            {areFormAnalyticsAvailable && record.entity.formInfo.status.state !== FormState.Draft && (
                                <Tooltip title="View analytics for this form" showArrow={false}>
                                    <Icon
                                        icon="TrendingUp"
                                        size="md"
                                        onClick={() => {
                                            history.push(
                                                `${PageRoutes.GOVERN_DASHBOARD}?documentationTab=analytics&tab=byForm&filter=${record.entity.urn}`,
                                            );
                                        }}
                                    />
                                </Tooltip>
                            )}
                            <Dropdown
                                menu={{ items }}
                                trigger={['click']}
                                data-testid={`${record.entity.urn}-actions-dropdown`}
                            >
                                <Icon icon="MoreVert" size="md" />
                            </Dropdown>
                        </CardIcons>
                    </>
                );
            },
        },
    ];

    return (
        <>
            <Table columns={formColumns} data={filteredForms} isLoading={isLoading} isScrollable />
            <ConfirmationModal
                isOpen={showConfirmDelete}
                handleClose={handleDeleteClose}
                handleConfirm={() => handleDeleteForm(currentForm)}
                modalTitle="Confirm Delete"
                modalText="Are you sure you want to delete the form?"
            />
            <ConfirmationModal
                isOpen={showConfirmUnpublish}
                handleClose={handleUnpublishClose}
                handleConfirm={() => handleUnpublishForm(currentForm)}
                modalTitle="Confirm Unpublish"
                modalText={UNPUBLISH_MODAL_TEXT}
                confirmButtonText="Unpublish"
            />
        </>
    );
};

export default FormsTable;
