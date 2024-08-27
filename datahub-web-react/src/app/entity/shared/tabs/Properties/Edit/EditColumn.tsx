import { Button } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import EditStructuredPropertyModal from './EditStructuredPropertyModal';
import { StructuredPropertyEntity } from '../../../../../../types.generated';

interface Props {
    structuredProperty?: StructuredPropertyEntity;
    associatedUrn?: string;
    values?: (string | number | null)[];
    refetch?: () => void;
}

export function EditColumn({ structuredProperty, associatedUrn, values, refetch }: Props) {
    const { t } = useTranslation();
    const [isEditModalVisible, setIsEditModalVisible] = useState(false);

    if (!structuredProperty || structuredProperty?.definition.immutable) {
        return null;
    }

    return (
        <>
            <Button type="link" onClick={() => setIsEditModalVisible(true)}>
                {t('common.edit')}
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
