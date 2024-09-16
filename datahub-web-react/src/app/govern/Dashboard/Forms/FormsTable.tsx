import { NetworkStatus } from '@apollo/client';
import { colors, Icon, Pill, Table, typography } from '@components';
import { AlignmentOptions, ColorOptions } from '@src/alchemy-components/theme/config';
import { toRelativeTimeString } from '@src/app/shared/time/timeUtils';
import { ConfirmationModal } from '@src/app/sharedV2/modals/ConfirmationModal';
import { showToastMessage, ToastType } from '@src/app/sharedV2/toastMessageUtils';
import { PageRoutes } from '@src/conf/Global';
import { useDeleteFormMutation } from '@src/graphql/form.generated';
import { useGetSearchResultsForMultipleQuery } from '@src/graphql/search.generated';
import { EntityType, FormState } from '@src/types.generated';
import { capitalizeFirstLetter } from '@src/app/shared/textUtil';
import { Dropdown, Tooltip, Typography } from 'antd';
import React, { useEffect, useState } from 'react';
import { useHistory } from 'react-router';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import EmptyForms from './EmptyForms';
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
    color: ${colors.gray[1600]};
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

const FormsTable = () => {
    const history = useHistory();
    const [showConfirmDelete, setShowConfirmDelete] = useState<boolean>(false);
    const [currentForm, setCurrentForm] = useState();

    const [deleteForm] = useDeleteFormMutation();

    const inputs = {
        types: [EntityType.Form],
        query: '*',
        start: 0,
        count: 100,
        searchFlags: { skipCache: true },
    };

    // Execute search
    const {
        data: searchData,
        loading,
        refetch,
        networkStatus,
    } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: inputs,
        },
        fetchPolicy: 'cache-first',
        notifyOnNetworkStatusChange: true,
    });

    const formsData = searchData?.searchAcrossEntities?.searchResults || [];

    const isLoading = loading || networkStatus === NetworkStatus.refetch;

    useEffect(() => {
        refetch();
    }, [refetch]);

    if (!isLoading && !formsData.length) {
        return <EmptyForms />;
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

    const formColumns = [
        {
            title: 'Name',
            key: 'name',
            render: (record) => {
                return (
                    <CellContainer>
                        <Link to={`/govern/dashboard/edit-form/${record.entity.urn}`}>
                            <FormName>{record.entity.formInfo.name}</FormName>
                        </Link>
                        <FormDescription> {record.entity.formInfo.description}</FormDescription>
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
                    <StatusContainer>
                        <Pill
                            label={status.charAt(0) + status.slice(1).toLowerCase()}
                            colorScheme={colorScheme}
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
                    record.entity.formInfo.status.state === FormState.Published &&
                    record.entity.formInfo.status?.lastModified?.time;
                return (
                    <CellContainer>
                        {publishedTime ? capitalizeFirstLetter(toRelativeTimeString(publishedTime)) : '-'}
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
                                    history.push(`/govern/dashboard/edit-form/${record.entity.urn}`);
                                }}
                            >
                                Edit
                            </MenuItem>
                        ),
                    },
                    {
                        key: '1',
                        label: (
                            <MenuItem
                                onClick={() => {
                                    setCurrentForm(record);
                                    setShowConfirmDelete(true);
                                }}
                            >
                                Delete
                            </MenuItem>
                        ),
                    },
                ];

                return (
                    <>
                        <CardIcons>
                            {record.entity.formInfo.status.state !== FormState.Draft && (
                                <Tooltip title="View analytics for this form">
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
                            <Dropdown menu={{ items }} trigger={['click']}>
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
            <Table columns={formColumns} data={formsData} isLoading={isLoading} isScrollable />
            <ConfirmationModal
                isOpen={showConfirmDelete}
                handleClose={handleDeleteClose}
                handleConfirm={() => handleDeleteForm(currentForm)}
                modalTitle="Confirm Delete"
                modalText="Are you sure you want to delete the form?"
            />
        </>
    );
};

export default FormsTable;
