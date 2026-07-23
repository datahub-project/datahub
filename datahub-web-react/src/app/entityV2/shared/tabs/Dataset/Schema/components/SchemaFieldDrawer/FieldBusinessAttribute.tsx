import React from 'react';
import { useTranslation } from 'react-i18next';

import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';
import { StyledDivider } from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/components';
import BusinessAttributeGroup from '@app/shared/businessAttribute/BusinessAttributeGroup';
import { useBusinessAttributesFlag } from '@app/useAppConfig';

import { EntityType, SchemaField } from '@types';

interface Props {
    expandedField: SchemaField;
    refetch?: () => void;
}

export default function FieldBusinessAttribute({ expandedField, refetch }: Props) {
    const { t } = useTranslation('entity.profile.schema');
    const businessAttributesFlag = useBusinessAttributesFlag();

    if (!businessAttributesFlag) {
        return null;
    }

    const businessAttributeContent = (
        <BusinessAttributeGroup
            businessAttribute={expandedField?.schemaFieldEntity?.businessAttributes?.businessAttribute || undefined}
            canRemove
            buttonProps={{ size: 'small' }}
            canAddAttribute
            entityUrn={expandedField?.schemaFieldEntity?.urn}
            entityType={EntityType.Dataset}
            entitySubresource={expandedField.fieldPath}
            highlightText=""
            refetch={refetch}
        />
    );

    return (
        <>
            <SidebarSection
                title={t('fieldBusinessAttribute.sectionTitle')}
                content={businessAttributeContent}
                collapsible
            />
            <StyledDivider dashed />
        </>
    );
}
