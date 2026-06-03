import { NetworkStatus } from '@apollo/client';
import { Table } from '@components';
import { message } from 'antd';
import React, { useCallback, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';

import {
    ApplicationActionsColumn,
    ApplicationDescriptionColumn,
    ApplicationNameColumn,
    ApplicationOwnersColumn,
} from '@app/applications/ApplicationsTableColumns';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';
import { AlignmentOptions } from '@src/alchemy-components/theme/config';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { GetSearchResultsForMultipleQuery } from '@src/graphql/search.generated';
import { EntityType } from '@src/types.generated';

import { useDeleteApplicationMutation } from '@graphql/application.generated';

interface Props {
    searchQuery: string;
    searchData: GetSearchResultsForMultipleQuery | undefined;
    loading: boolean;
    networkStatus: NetworkStatus;
    refetch: () => Promise<any>;
}

const ApplicationsTable = ({ searchQuery, searchData, loading: propLoading, networkStatus, refetch }: Props) => {
    const { t } = useTranslation('misc');
    const { t: tc } = useTranslation('common.actions');
    const { t: tl } = useTranslation('common.labels');
    const entityRegistry = useEntityRegistry();
    const [deleteApplicationMutation] = useDeleteApplicationMutation();

    // Optimize the applicationsData with useMemo to prevent unnecessary filtering on re-renders
    const applicationsData = useMemo(() => {
        return searchData?.searchAcrossEntities?.searchResults || [];
    }, [searchData]);

    // Simplified state for delete confirmation modal
    const [showDeleteModal, setShowDeleteModal] = useState(false);
    const [applicationUrnToDelete, setApplicationUrnToDelete] = useState('');
    const [applicationDisplayName, setApplicationDisplayName] = useState('');

    // Filter applications based on search query and sort by name - optimized with useMemo
    const filteredApplications = useMemo(() => {
        return applicationsData
            .filter((result) => {
                const application = result.entity;
                const displayName = entityRegistry.getDisplayName(EntityType.Application, application);
                if (!searchQuery) return true;
                return displayName.toLowerCase().includes(searchQuery.toLowerCase());
            })
            .sort((a, b) => {
                const nameA = entityRegistry.getDisplayName(EntityType.Application, a.entity);
                const nameB = entityRegistry.getDisplayName(EntityType.Application, b.entity);
                return nameA.localeCompare(nameB);
            });
    }, [applicationsData, searchQuery, entityRegistry]);

    const isLoading = propLoading || networkStatus === NetworkStatus.refetch;

    // Simplified function to initiate tag deletion
    const showDeleteConfirmation = useCallback(
        (applicationUrn: string) => {
            // Find the application entity from applicationsData
            const applicationData = applicationsData.find((result) => result.entity.urn === applicationUrn);
            if (!applicationData) {
                message.error(t('applications.findInfoError'));
                return;
            }

            const fullDisplayName = entityRegistry.getDisplayName(EntityType.Application, applicationData.entity);

            setApplicationUrnToDelete(applicationUrn);
            setApplicationDisplayName(fullDisplayName);
            setShowDeleteModal(true);
        },
        [entityRegistry, applicationsData, t],
    );

    // Function to handle the actual application deletion
    const handleDeleteApplication = useCallback(() => {
        deleteApplicationMutation({
            variables: {
                urn: applicationUrnToDelete,
            },
        })
            .then(() => {
                message.success(t('applications.deleteSuccess', { name: applicationDisplayName }));
                refetch(); // Refresh the application list
            })
            .catch((e: any) => {
                message.error(t('applications.deleteError', { error: e.message }));
            });

        setShowDeleteModal(false);
        setApplicationUrnToDelete('');
        setApplicationDisplayName('');
    }, [deleteApplicationMutation, refetch, applicationUrnToDelete, applicationDisplayName, t]);

    const handleDeleteClose = useCallback(() => {
        setShowDeleteModal(false);
        setApplicationUrnToDelete('');
        setApplicationDisplayName('');
    }, []);

    const columns = useMemo(
        () => [
            {
                title: t('applications.columnApplication'),
                key: 'application',
                render: (record) => {
                    const application = record.entity;
                    const displayName = entityRegistry.getDisplayName(EntityType.Application, application);
                    return (
                        <ApplicationNameColumn
                            applicationUrn={application.urn}
                            displayName={displayName}
                            searchQuery={searchQuery}
                        />
                    );
                },
            },
            {
                title: tl('description'),
                key: 'description',
                render: (record) => {
                    return (
                        <ApplicationDescriptionColumn
                            key={`description-${record.entity.urn}`}
                            applicationUrn={record.entity.urn}
                            description={record.entity.properties?.description}
                        />
                    );
                },
            },
            {
                title: t('applications.columnOwners'),
                key: 'owners',
                render: (record) => {
                    return (
                        <ApplicationOwnersColumn key={`owners-${record.entity.urn}`} owners={record.entity.ownership} />
                    );
                },
            },
            {
                title: '',
                key: 'actions',
                alignment: 'right' as AlignmentOptions,
                render: (record) => {
                    return (
                        <ApplicationActionsColumn
                            applicationUrn={record.entity.urn}
                            onDelete={() => {
                                showDeleteConfirmation(record.entity.urn);
                            }}
                        />
                    );
                },
            },
        ],
        [entityRegistry, searchQuery, showDeleteConfirmation, t, tl],
    );

    // Generate table data once with memoization
    const tableData = useMemo(() => {
        return filteredApplications.map((application) => ({
            ...application,
            key: application.entity.urn,
        }));
    }, [filteredApplications]);

    return (
        <>
            <Table columns={columns} data={tableData} isLoading={isLoading} isScrollable rowKey="key" />
            <ConfirmationModal
                isOpen={showDeleteModal}
                handleClose={handleDeleteClose}
                handleConfirm={handleDeleteApplication}
                modalTitle={t('applications.deleteModalTitle')}
                modalText={t('applications.deleteConfirmation', { name: applicationDisplayName })}
                closeButtonText={tc('cancel')}
                confirmButtonText={tc('delete')}
            />
        </>
    );
};

export default ApplicationsTable;
