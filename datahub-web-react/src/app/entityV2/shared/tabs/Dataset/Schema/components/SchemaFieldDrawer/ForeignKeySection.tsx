import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import translateFieldPath from '@app/entityV2/dataset/profile/schema/utils/translateFieldPath';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';
import { StyledDivider } from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/components';
import getFieldForeignKeyConstraints, {
    ForeignKeySchemaMetadata,
    decodeForeignKeyFieldPath,
} from '@app/entityV2/shared/tabs/Dataset/Schema/utils/getFieldForeignKeyConstraints';
import { CompactEntityNameComponent } from '@app/recommendations/renderer/component/CompactEntityNameComponent';

import { SchemaField } from '@types';

const ConstraintBody = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

const FieldPairs = styled.table`
    width: 100%;
    table-layout: fixed;
    border-collapse: collapse;

    th,
    td {
        padding: 0 12px 2px 0;
        text-align: left;
        vertical-align: top;
        overflow-wrap: anywhere;
    }
`;

const ColumnTitle = styled.th.attrs({ scope: 'col' })`
    font-size: 12px;
    font-weight: 600;
    color: ${(props) => props.theme.colors.textSecondary};
`;

const FieldName = styled.td`
    font-size: 12px;
    color: ${(props) => props.theme.colors.text};
`;

interface Props {
    expandedField: SchemaField;
    schemaMetadata?: ForeignKeySchemaMetadata | null;
}

function formatFieldName(fieldPath?: string | null) {
    return decodeForeignKeyFieldPath(translateFieldPath(fieldPath ?? ''));
}

export default function ForeignKeySection({ expandedField, schemaMetadata }: Props) {
    const { t } = useTranslation('entity.profile.schema');
    const constraints = getFieldForeignKeyConstraints(schemaMetadata, expandedField.fieldPath);
    const sections = constraints.flatMap((constraint, index) => {
        // The structural schema query leaves out foreignDataset until the full schema arrives.
        const { foreignDataset, sourceFields, foreignFields } = constraint;
        if (!foreignDataset) return [];
        const pairCount = Math.max(sourceFields?.length ?? 0, foreignFields?.length ?? 0);

        return [
            <SidebarSection
                key={constraint.name ?? index}
                title={t('fieldForeignKey.foreignKeyTo')}
                collapsible={false}
                content={
                    <ConstraintBody data-testid={`foreign-key-${constraint.name ?? index}`}>
                        <CompactEntityNameComponent entity={foreignDataset} showFullTooltip />
                        <FieldPairs>
                            <thead>
                                <tr>
                                    <ColumnTitle>{t('fieldForeignKey.sourceFields')}</ColumnTitle>
                                    <ColumnTitle>{t('fieldForeignKey.targetFields')}</ColumnTitle>
                                </tr>
                            </thead>
                            <tbody>
                                {Array.from({ length: pairCount }, (_, position) => (
                                    // eslint-disable-next-line react/no-array-index-key -- position identifies the pair
                                    <tr key={position}>
                                        <FieldName>{formatFieldName(sourceFields?.[position]?.fieldPath)}</FieldName>
                                        <FieldName>{formatFieldName(foreignFields?.[position]?.fieldPath)}</FieldName>
                                    </tr>
                                ))}
                            </tbody>
                        </FieldPairs>
                    </ConstraintBody>
                }
            />,
        ];
    });

    if (!sections.length) return null;

    return (
        <>
            {sections}
            <StyledDivider />
        </>
    );
}
