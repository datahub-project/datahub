import { Button } from 'antd';
import React, { useState } from 'react';
import { PropertyRow } from '../types';
import EditStructuredPropertyModal from './EditStructuredPropertyModal';

interface Props {
    propertyRow: PropertyRow;
}

export function EditColumn({ propertyRow }: Props) {
    const [isEditModalVisible, setIsEditModalVisible] = useState(false);

    if (!propertyRow.structuredProperty || propertyRow.structuredProperty?.definition.immutable) {
        return null;
    }

    return (
        <>
            <Button type="link" onClick={() => setIsEditModalVisible(true)}>
                Edit
            </Button>
            <EditStructuredPropertyModal
                isOpen={isEditModalVisible}
                propertyRow={propertyRow}
                structuredProperty={propertyRow.structuredProperty}
                closeModal={() => setIsEditModalVisible(false)}
            />
        </>
    );
}
