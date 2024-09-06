import { Button } from 'antd';
import React, { useState } from 'react';
import EditStructuredPropertyModal from './EditStructuredPropertyModal';
import { StructuredPropertyEntity } from '../../../../../../types.generated';

interface Props {
    structuredProperty?: StructuredPropertyEntity;
    associatedUrn?: string;
    values?: (string | number | null)[];
    refetch?: () => void;
}

export function EditColumn({ structuredProperty, associatedUrn, values, refetch }: Props) {
    const [isEditModalVisible, setIsEditModalVisible] = useState(false);

    if (!structuredProperty || structuredProperty?.definition.immutable) {
        return null;
    }

    return (
        <>
            <Button type="link" onClick={() => setIsEditModalVisible(true)}>
                Edit
            </Button>
            <EditStructuredPropertyModal
                isOpen={isEditModalVisible}
                structuredProperty={structuredProperty}
                associatedUrn={associatedUrn}
                values={values}
                closeModal={() => setIsEditModalVisible(false)}
                refetch={refetch}
            />
        </>
    );
}
