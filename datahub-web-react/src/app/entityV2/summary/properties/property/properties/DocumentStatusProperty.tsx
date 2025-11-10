import { Tooltip } from '@components';
import React from 'react';
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
    const { canChangeStatus } = useDocumentPermissions(urn);
    const { updateStatus } = useUpdateDocument();

    const status = document?.info?.status?.state;

    const handleStatusChange = async (values: string[]) => {
        const newStatus = values[0] as DocumentState;
        console.log('[DocumentStatusProperty] Updating status to:', newStatus);
        await updateStatus({
            urn,
            state: newStatus,
        });
        console.log('[DocumentStatusProperty] Status updated, calling refetch...');
        await refetch();
        console.log('[DocumentStatusProperty] Refetch complete!');
    };

    const renderValue = () => {
        if (!status) return <span>-</span>;

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
                            values={[status]}
                            onUpdate={handleStatusChange}
                            isDisabled={!canChangeStatus}
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

    return <BaseProperty {...props} values={status ? [status] : []} renderValue={renderValue} maxValues={1} />;
}
