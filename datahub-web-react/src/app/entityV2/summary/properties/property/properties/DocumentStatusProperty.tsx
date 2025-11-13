import { Tooltip } from '@components';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import { useDocumentPermissions } from '@app/documentV2/hooks/useDocumentPermissions';
import { useUpdateDocument } from '@app/documentV2/hooks/useUpdateDocument';
import { useEntityData, useRefetch } from '@app/entity/shared/EntityContext';
import BaseProperty from '@app/entityV2/summary/properties/property/properties/BaseProperty';
import { PropertyComponentProps } from '@app/entityV2/summary/properties/types';
import { SimpleSelect } from '@src/alchemy-components';

import { Document, DocumentState } from '@types';

const StatusSelectWrapper = styled.div``;

const statusOptions = [
    {
        label: 'Published',
        value: DocumentState.Published,
    },
    {
        label: 'Draft',
        value: DocumentState.Unpublished,
    },
];

export default function DocumentStatusProperty(props: PropertyComponentProps) {
    const { urn, entityData } = useEntityData();
    const document = entityData as Document;
    const refetch = useRefetch();
    const { canEditState } = useDocumentPermissions(urn);
    const { updateStatus } = useUpdateDocument();

    const serverStatus = document?.info?.status?.state;
    const [optimisticStatus, setOptimisticStatus] = useState<DocumentState | undefined>(serverStatus);

    // Sync optimistic state with server state when it changes
    useEffect(() => {
        setOptimisticStatus(serverStatus);
    }, [serverStatus]);

    const handleStatusChange = async (values: string[]) => {
        const newStatus = values[0] as DocumentState;
        const previousStatus = optimisticStatus;

        // Optimistically update the UI immediately
        setOptimisticStatus(newStatus);

        try {
            await updateStatus({
                urn,
                state: newStatus,
            });
            await refetch();
        } catch (error) {
            // Revert to previous status if the mutation fails
            console.error('[DocumentStatusProperty] Update failed, reverting to:', previousStatus);
            setOptimisticStatus(previousStatus);
        }
    };

    const renderValue = () => {
        if (!optimisticStatus) return <span>-</span>;

        return (
            <StatusSelectWrapper>
                <Tooltip
                    title={
                        <>
                            Publish this document to make it visible to <b>Ask DataHub</b>
                        </>
                    }
                    placement="top"
                >
                    <div>
                        <SimpleSelect
                            values={[optimisticStatus]}
                            onUpdate={handleStatusChange}
                            isDisabled={!canEditState}
                            options={statusOptions}
                            size="sm"
                            width="fit-content"
                            showClear={false}
                        />
                    </div>
                </Tooltip>
            </StatusSelectWrapper>
        );
    };

    return (
        <BaseProperty
            {...props}
            values={optimisticStatus ? [optimisticStatus] : []}
            renderValue={renderValue}
            maxValues={1}
        />
    );
}
