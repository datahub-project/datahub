import { NetworkStatus } from '@apollo/client';
import { Modal, Table } from '@components';
import { message } from 'antd';
import React, { useCallback, useMemo, useState } from 'react';

import {
    ApplicationActionsColumn,
    ApplicationDescriptionColumn,
    ApplicationNameColumn,
    ApplicationOwnersColumn,
} from '@app/applications/ApplicationsTableColumns';
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

    const [sortedInfo, setSortedInfo] = useState<{
        columnKey?: string;
        order?: 'ascend' | 'descend';
    }>({});

    // Fix the handler type to match what Table expects
    const handleTableChange = (pagination: any, filters: any, sorter: any): void => {
        setSortedInfo(sorter);
    };

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
                message.error('Failed to find application information');
                return;
            }

            const fullDisplayName = entityRegistry.getDisplayName(EntityType.Application, applicationData.entity);

            setApplicationUrnToDelete(applicationUrn);
            setApplicationDisplayName(fullDisplayName);
            setShowDeleteModal(true);
        },
        [entityRegistry, applicationsData],
    );

    // Function to handle the actual application deletion
    const handleDeleteApplication = useCallback(() => {
        deleteApplicationMutation({
            variables: {
                urn: applicationUrnToDelete,
            },
        })
            .then(() => {
                message.success(`Application "${applicationDisplayName}" has been deleted`);
                refetch(); // Refresh the application list
            })
            .catch((e: any) => {
                message.error(`Failed to delete tag: ${e.message}`);
            });

        setShowDeleteModal(false);
    }, [deleteApplicationMutation, refetch, applicationUrnToDelete, applicationDisplayName]);

    const columns = useMemo(
        () => [
            {
                title: 'Application',
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
                sorter: (sourceA, sourceB) => {
                    const nameA = entityRegistry.getDisplayName(EntityType.Application, sourceA.entity);
                    const nameB = entityRegistry.getDisplayName(EntityType.Application, sourceB.entity);
                    return nameA.localeCompare(nameB);
                },
                sortOrder: sortedInfo.columnKey === 'tag' ? sortedInfo.order : null,
            },
            {
                title: 'Description',
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
                title: 'Owners',
                key: 'owners',
                render: (record) => {
                    return (
                        <ApplicationOwnersColumn
                            key={`owners-${record.entity.urn}`}
                            applicationUrn={record.entity.urn}
                            owners={record.entity.ownership}
                        />
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
        [entityRegistry, searchQuery, sortedInfo, showDeleteConfirmation],
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
            <Table
                columns={columns}
                data={tableData}
                isLoading={isLoading}
                isScrollable
                rowKey="key"
                onChange={handleTableChange as any}
            />

            {/* Delete confirmation modal - simplified */}
            <Modal
                title={`Delete application ${applicationDisplayName}`}
                onCancel={() => setShowDeleteModal(false)}
                open={showDeleteModal}
                centered
                buttons={[
                    {
                        text: 'Cancel',
                        color: 'violet',
                        variant: 'text',
                        onClick: () => setShowDeleteModal(false),
                    },
                    {
                        text: 'Delete',
                        color: 'red',
                        variant: 'filled',
                        onClick: handleDeleteApplication,
                    },
                ]}
            >
                <p>Are you sure you want to delete this application? This action cannot be undone.</p>
            </Modal>
        </>
    );
};

export default ApplicationsTable;
