import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useBaseEntity } from '@app/entity/shared/EntityContext';
import TypeLabel from '@app/entityV2/shared/tabs/Dataset/Schema/components/TypeLabel';

import { GetApiQuery } from '@graphql/api.generated';
import { SchemaFieldDataType } from '@types';

const TabContent = styled.div`
    padding: 20px;
    display: flex;
    flex-direction: column;
    gap: 24px;
`;

const Section = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

const SectionTitle = styled.div`
    font-size: 16px;
    font-weight: 600;
    color: ${(props) => props.theme.colors.text};
`;

const Table = styled.table`
    width: 100%;
    border-collapse: collapse;
    border: 1px solid ${(props) => props.theme.colors.border};
    border-radius: 8px;
    overflow: hidden;
`;

const HeaderCell = styled.th`
    text-align: left;
    padding: 8px 12px;
    font-size: 12px;
    font-weight: 600;
    color: ${(props) => props.theme.colors.textSecondary};
    background: ${(props) => props.theme.colors.bgSurface};
    border-bottom: 1px solid ${(props) => props.theme.colors.border};
`;

const Row = styled.tr`
    &:not(:last-child) td {
        border-bottom: 1px solid ${(props) => props.theme.colors.border};
    }
`;

const Cell = styled.td`
    padding: 8px 12px;
    font-size: 14px;
    color: ${(props) => props.theme.colors.text};
    vertical-align: top;
`;

const ParamName = styled.span`
    font-family: 'Roboto Mono', monospace;
    color: ${(props) => props.theme.colors.text};
`;

const RequiredBadge = styled.span<{ $required: boolean }>`
    font-size: 12px;
    font-weight: 600;
    color: ${(props) => (props.$required ? props.theme.colors.textBrand : props.theme.colors.textSecondary)};
`;

const SecondaryText = styled.span`
    color: ${(props) => props.theme.colors.textSecondary};
`;

const EmptyText = styled.div`
    font-size: 13px;
    color: ${(props) => props.theme.colors.textSecondary};
`;

const ReturnsRow = styled.div`
    display: flex;
    align-items: center;
    gap: 6px;
`;

type SchemaFieldType = NonNullable<
    NonNullable<Extract<GetApiQuery['entity'], { __typename?: 'Api' }>['signature']>['inputFields']
>[number];

function FieldTypeLabel({ field }: { field: SchemaFieldType }) {
    // SchemaFieldDataType.Null makes TypeLabel fall back to rendering nativeDataType
    return <TypeLabel type={field.type ?? SchemaFieldDataType.Null} nativeDataType={field.nativeDataType} />;
}

function OutputSection({ fields }: { fields: SchemaFieldType[] }) {
    const { t } = useTranslation('entity.types');
    if (fields.length === 0) {
        return <EmptyText>{t('api.signature.noOutput')}</EmptyText>;
    }
    if (fields.length === 1) {
        return (
            <ReturnsRow>
                <EmptyText>{t('api.signature.returns')}:</EmptyText>
                <FieldTypeLabel field={fields[0]} />
            </ReturnsRow>
        );
    }
    return <FieldsTable fields={fields} />;
}

function FieldsTable({ fields }: { fields: SchemaFieldType[] }) {
    const { t } = useTranslation('entity.types');
    return (
        <Table>
            <thead>
                <tr>
                    <HeaderCell>{t('api.signature.tableParameter')}</HeaderCell>
                    <HeaderCell>{t('api.signature.tableType')}</HeaderCell>
                    <HeaderCell>{t('api.signature.tableRequired')}</HeaderCell>
                    <HeaderCell>{t('api.signature.tableDescription')}</HeaderCell>
                </tr>
            </thead>
            <tbody>
                {fields.map((field) => {
                    const required = field.nullable === false;
                    return (
                        <Row key={field.fieldPath}>
                            <Cell>
                                <ParamName>{field.fieldPath}</ParamName>
                            </Cell>
                            <Cell>
                                <FieldTypeLabel field={field} />
                            </Cell>
                            <Cell>
                                <RequiredBadge $required={required}>
                                    {required ? t('api.signature.valueRequired') : t('api.signature.valueOptional')}
                                </RequiredBadge>
                            </Cell>
                            <Cell>{field.description || <SecondaryText>&mdash;</SecondaryText>}</Cell>
                        </Row>
                    );
                })}
            </tbody>
        </Table>
    );
}

export default function SignatureTab() {
    const baseEntity = useBaseEntity<GetApiQuery>();
    const { t } = useTranslation('entity.types');
    const signature = baseEntity?.entity?.__typename === 'Api' ? baseEntity.entity.signature : undefined;

    const inputFields = (signature?.inputFields ?? []) as SchemaFieldType[];
    const outputFields = (signature?.outputFields ?? []) as SchemaFieldType[];

    return (
        <TabContent>
            <Section>
                <SectionTitle>{t('api.signature.inputTitle')}</SectionTitle>
                {inputFields.length === 0 ? (
                    <EmptyText>{t('api.signature.noInput')}</EmptyText>
                ) : (
                    <FieldsTable fields={inputFields} />
                )}
            </Section>
            <Section>
                <SectionTitle>{t('api.signature.outputTitle')}</SectionTitle>
                <OutputSection fields={outputFields} />
            </Section>
        </TabContent>
    );
}
