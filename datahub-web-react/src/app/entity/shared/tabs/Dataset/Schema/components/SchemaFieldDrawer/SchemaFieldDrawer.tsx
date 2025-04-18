import { Drawer } from 'antd';
import React, { useMemo } from 'react';
import styled from 'styled-components';

import { pathMatchesNewPath } from '@app/entity/dataset/profile/schema/utils/utils';
import DrawerHeader from '@app/entity/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/DrawerHeader';
import FieldAttribute from '@app/entity/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/FieldAttribute';
import FieldDescription from '@app/entity/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/FieldDescription';
import FieldHeader from '@app/entity/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/FieldHeader';
import FieldProperties from '@app/entity/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/FieldProperties';
import FieldTags from '@app/entity/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/FieldTags';
import FieldTerms from '@app/entity/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/FieldTerms';
import FieldUsageStats from '@app/entity/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/FieldUsageStats';
import useGetSchemaColumnProperties from '@app/entity/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/useGetSchemaColumnProperties';
import SidebarStructuredPropsSection from '@src/app/entity/shared/containers/profile/sidebar/StructuredProperties/SidebarStructuredPropsSection';

import { EditableSchemaMetadata, SchemaField } from '@types';

const StyledDrawer = styled(Drawer)`
    position: absolute;

    &&& .ant-drawer-body {
        padding: 0;
    }

    &&& .ant-drawer-content-wrapper {
        border-left: 3px solid ${(props) => props.theme.styles['primary-color']};
    }
`;

const MetadataSections = styled.div`
    padding: 16px 24px;
`;

interface Props {
    schemaFields: SchemaField[];
    editableSchemaMetadata?: EditableSchemaMetadata | null;
    expandedDrawerFieldPath: string | null;
    setExpandedDrawerFieldPath: (fieldPath: string | null) => void;
}

export default function SchemaFieldDrawer({
    schemaFields,
    editableSchemaMetadata,
    expandedDrawerFieldPath,
    setExpandedDrawerFieldPath,
}: Props) {
    const expandedFieldIndex = useMemo(
        () => schemaFields.findIndex((row) => row.fieldPath === expandedDrawerFieldPath),
        [expandedDrawerFieldPath, schemaFields],
    );
    const expandedField =
        expandedFieldIndex !== undefined && expandedFieldIndex !== -1 ? schemaFields[expandedFieldIndex] : undefined;
    const editableFieldInfo = editableSchemaMetadata?.editableSchemaFieldInfo?.find((candidateEditableFieldInfo) =>
        pathMatchesNewPath(candidateEditableFieldInfo.fieldPath, expandedField?.fieldPath),
    );
    const schemaColumnProperties = useGetSchemaColumnProperties();

    return (
        <StyledDrawer
            open={!!expandedDrawerFieldPath}
            onClose={() => setExpandedDrawerFieldPath(null)}
            getContainer={() => document.getElementById('entity-profile-sidebar') as HTMLElement}
            contentWrapperStyle={{ width: '100%', boxShadow: 'none' }}
            mask={false}
            maskClosable={false}
            placement="right"
            closable={false}
        >
            <DrawerHeader
                setExpandedDrawerFieldPath={setExpandedDrawerFieldPath}
                schemaFields={schemaFields}
                expandedFieldIndex={expandedFieldIndex}
            />
            {expandedField && (
                <>
                    <FieldHeader expandedField={expandedField} />
                    <MetadataSections>
                        <FieldDescription expandedField={expandedField} editableFieldInfo={editableFieldInfo} />
                        <FieldUsageStats expandedField={expandedField} />
                        <FieldTags expandedField={expandedField} editableSchemaMetadata={editableSchemaMetadata} />
                        <FieldTerms expandedField={expandedField} editableSchemaMetadata={editableSchemaMetadata} />
                        <SidebarStructuredPropsSection
                            properties={{ schemaField: expandedField, schemaColumnProperties }}
                        />
                        <FieldProperties
                            expandedField={expandedField}
                            schemaColumnProperties={schemaColumnProperties}
                        />
                        <FieldAttribute expandedField={expandedField} />
                    </MetadataSections>
                </>
            )}
        </StyledDrawer>
    );
}
