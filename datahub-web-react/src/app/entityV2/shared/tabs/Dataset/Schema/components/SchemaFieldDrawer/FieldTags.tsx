import React from 'react';
import { useTranslation } from 'react-i18next';

import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';
import { StyledDivider } from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/components';
import useTagsAndTermsRenderer from '@app/entityV2/shared/tabs/Dataset/Schema/utils/useTagsAndTermsRenderer';
import SchemaEditableContext from '@app/shared/SchemaEditableContext';

import { EditableSchemaMetadata, GlobalTags, SchemaField } from '@types';

interface Props {
    expandedField: SchemaField;
    editableSchemaMetadata?: EditableSchemaMetadata | null;
}

export default function FieldTags({ expandedField, editableSchemaMetadata }: Props) {
    const { t } = useTranslation('entity.profile.schema');
    const isSchemaEditable = React.useContext(SchemaEditableContext);
    const tagRenderer = useTagsAndTermsRenderer(
        editableSchemaMetadata,
        {
            showTags: true,
            showTerms: false,
        },
        '',
        isSchemaEditable,
    );

    return (
        <>
            <SidebarSection
                title={t('fieldTags.sectionTitle')}
                content={tagRenderer(expandedField.globalTags as GlobalTags, expandedField)}
            />
            <StyledDivider dashed />
        </>
    );
}
