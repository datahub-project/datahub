import { Drawer } from 'antd';
import React, { useMemo } from 'react';
import styled from 'styled-components';
import SidebarStructuredPropsSection from '@src/app/entity/shared/containers/profile/sidebar/StructuredProperties/SidebarStructuredPropsSection';
import DrawerHeader from './DrawerHeader';
import FieldHeader from './FieldHeader';
import FieldDescription from './FieldDescription';
import { EditableSchemaMetadata, SchemaField } from '../../../../../../../../types.generated';
import { pathMatchesNewPath } from '../../../../../../dataset/profile/schema/utils/utils';
import FieldUsageStats from './FieldUsageStats';
import FieldTags from './FieldTags';
import FieldTerms from './FieldTerms';
import FieldProperties from './FieldProperties';
import FieldAttribute from './FieldAttribute';
import useGetSchemaColumnProperties from './useGetSchemaColumnProperties';

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
