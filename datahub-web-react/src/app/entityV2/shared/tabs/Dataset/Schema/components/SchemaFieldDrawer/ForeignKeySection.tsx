import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import translateFieldPath from '@app/entityV2/dataset/profile/schema/utils/translateFieldPath';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';
import { StyledDivider } from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/components';
import getFieldForeignKeyConstraints from '@app/entityV2/shared/tabs/Dataset/Schema/utils/getFieldForeignKeyConstraints';
import { CompactEntityNameComponent } from '@app/recommendations/renderer/component/CompactEntityNameComponent';

import { ForeignKeyConstraint, SchemaField, SchemaMetadata } from '@types';

const ConstraintBody = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

const FieldColumns = styled.div`
    display: flex;
    gap: 24px;
`;

const FieldColumn = styled.div`
    display: flex;
    flex-direction: column;
    gap: 2px;
    min-width: 0;
`;

const ColumnTitle = styled.div`
    font-size: 12px;
    font-weight: 600;
    color: ${(props) => props.theme.colors.textSecondary};
`;

const FieldName = styled.div`
    font-size: 12px;
    color: ${(props) => props.theme.colors.text};
    overflow: hidden;
    text-overflow: ellipsis;
`;

const Unavailable = styled.div`
    font-size: 12px;
    color: ${(props) => props.theme.colors.textTertiary};
`;

interface Props {
    expandedField: SchemaField;
    schemaMetadata?: SchemaMetadata | null;
}

function renderFieldNames(fields: ForeignKeyConstraint['sourceFields']) {
    return (fields ?? []).map((field) => (
        <FieldName key={field?.fieldPath}>{translateFieldPath(field?.fieldPath ?? '')}</FieldName>
    ));
}

export default function ForeignKeySection({ expandedField, schemaMetadata }: Props) {
    const { t } = useTranslation('entity.profile.schema');
    const constraints = getFieldForeignKeyConstraints(schemaMetadata, expandedField.fieldPath);

    if (!constraints.length) {
        return null;
    }

    return (
        <>
            {constraints.map((constraint) => (
                <SidebarSection
                    key={constraint.name}
                    title={t('fieldForeignKey.foreignKeyTo')}
                    content={
                        <ConstraintBody data-testid={`foreign-key-${constraint.name}`}>
                            {constraint.foreignDataset ? (
                                <CompactEntityNameComponent entity={constraint.foreignDataset} showFullTooltip />
                            ) : (
                                <Unavailable>{t('fieldForeignKey.targetUnavailable')}</Unavailable>
                            )}
                            <FieldColumns>
                                <FieldColumn>
                                    <ColumnTitle>{t('fieldForeignKey.sourceFields')}</ColumnTitle>
                                    {renderFieldNames(constraint.sourceFields)}
                                </FieldColumn>
                                <FieldColumn>
                                    <ColumnTitle>{t('fieldForeignKey.targetFields')}</ColumnTitle>
                                    {renderFieldNames(constraint.foreignFields)}
                                </FieldColumn>
                            </FieldColumns>
                        </ConstraintBody>
                    }
                />
            ))}
            <StyledDivider />
        </>
    );
}
