import { Drawer } from 'antd';
import React, { useMemo } from 'react';
import styled from 'styled-components';
import { EditableSchemaMetadata, SchemaField } from '../../../../../../../../types.generated';
import DrawerFooter from './DrawerFooter';
import FieldHeader from './FieldHeader';

const StyledDrawer = styled(Drawer)`
    &&& .ant-drawer-body {
        padding: 0;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
        height: 100%;
    }

    &&& .ant-drawer-content-wrapper {
        box-shadow: -20px 0px 44px 0px rgba(0, 0, 0, 0.1);
    }
`;

const DrawerContent = styled.div`
    display: flex;
    flex-direction: column;
    justify-content: space-between;
    height: 100%;
`;
// const MetadataSections = styled.div`
//     padding: 16px 24px;
// `;
// const StatsSection = styled.div`
//     padding: 16px 24px;
//     background: ${REDESIGN_COLORS.BACKGROUND_GREY};
//     display: flex;
//     flex-direction: column;
// `;

interface Props {
    schemaFields: SchemaField[];
    editableSchemaMetadata?: EditableSchemaMetadata | null;
    expandedDrawerFieldPath: string | null;
    setExpandedDrawerFieldPath: (fieldPath: string | null) => void;
    // eslint-disable-next-line  react/no-unused-prop-types
    openTimelineDrawer?: boolean;
}

export default function TimelineDrawer({
    schemaFields,
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
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
    // const editableFieldInfo = editableSchemaMetadata?.editableSchemaFieldInfo.find((candidateEditableFieldInfo) =>
    //     pathMatchesNewPath(candidateEditableFieldInfo.fieldPath, expandedField?.fieldPath),
    // );

    return (
        <StyledDrawer
            open={!!expandedDrawerFieldPath}
            onClose={() => setExpandedDrawerFieldPath(null)}
            getContainer={() => document.getElementById('entity-profile-sidebar') as HTMLElement}
            contentWrapperStyle={{ width: '33%' }}
            mask={false}
            maskClosable={false}
            placement="right"
            closable={false}
            autoFocus={false}
        >
            {expandedField && (
                <DrawerContent>
                    <FieldHeader
                        setExpandedDrawerFieldPath={setExpandedDrawerFieldPath}
                        expandedField={expandedField}
                    />

                    <DrawerFooter
                        setExpandedDrawerFieldPath={setExpandedDrawerFieldPath}
                        schemaFields={schemaFields}
                        expandedFieldIndex={expandedFieldIndex}
                    />
                </DrawerContent>
            )}
        </StyledDrawer>
    );
}
