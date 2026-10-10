import React from 'react';
import { useTranslation } from 'react-i18next';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { useBaseEntity } from '@app/entity/shared/EntityContext';
import TypeLabel from '@app/entityV2/shared/tabs/Dataset/Schema/components/TypeLabel';
import PlatformIcon from '@app/sharedV2/icons/PlatformIcon';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { GetApiQuery } from '@graphql/api.generated';
import { EntityType, SchemaFieldDataType } from '@types';

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

const DatasetRefRow = styled.div`
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 8px;
`;

const DatasetChip = styled(Link)`
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 4px 10px 4px 6px;
    border-radius: 8px;
    border: 1px solid ${(props) => props.theme.colors.border};
    font-size: 13px;
    font-weight: 600;
    color: ${(props) => props.theme.colors.hyperlinks};
    &:hover {
        text-decoration: underline;
    }
`;

const DatasetSubtype = styled.span`
    font-size: 11px;
    font-weight: 600;
    color: ${(props) => props.theme.colors.textSecondary};
`;

type ApiSignatureType = NonNullable<Extract<GetApiQuery['entity'], { __typename?: 'Api' }>['signature']>;

type SchemaFieldType = NonNullable<ApiSignatureType['inputFields']>[number];

type DatasetRefType = NonNullable<ApiSignatureType['inputDatasets']>[number];

/**
 * The schema-by-reference view of one side of the signature: the cataloged
 * datasets (e.g. protobuf messages) whose schema defines what the API accepts
 * or returns. Each chip links to the dataset, where the full schema lives.
 */
function DatasetRefs({ datasets }: { datasets: DatasetRefType[] }) {
    const { t } = useTranslation('entity.types');
    const entityRegistry = useEntityRegistry();
    return (
        <DatasetRefRow data-testid="api-signature-dataset-refs">
            <EmptyText>
                {datasets.length === 1 ? t('api.signature.definedByDataset') : t('api.signature.definedByDatasets')}:
            </EmptyText>
            {datasets.map((dataset) => {
                const subType = dataset.subTypes?.typeNames?.[0];
                return (
                    <DatasetChip key={dataset.urn} to={entityRegistry.getEntityUrl(EntityType.Dataset, dataset.urn)}>
                        <PlatformIcon platform={dataset.platform} size={14} />
                        {entityRegistry.getDisplayName(EntityType.Dataset, dataset)}
                        {subType && <DatasetSubtype>{subType}</DatasetSubtype>}
                    </DatasetChip>
                );
            })}
        </DatasetRefRow>
    );
}

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
    const inputDatasets = (signature?.inputDatasets ?? []) as DatasetRefType[];
    const outputDatasets = (signature?.outputDatasets ?? []) as DatasetRefType[];

    // When a side is defined by reference, the dataset's schema is authoritative
    // and any inline fields are a cached view — so the datasets lead and the
    // field table (if any) follows.
    return (
        <TabContent>
            <Section>
                <SectionTitle>
                    {inputDatasets.length > 0 ? t('api.signature.inputDatasetsTitle') : t('api.signature.inputTitle')}
                </SectionTitle>
                {inputDatasets.length > 0 && <DatasetRefs datasets={inputDatasets} />}
                {inputFields.length > 0 && <FieldsTable fields={inputFields} />}
                {inputDatasets.length === 0 && inputFields.length === 0 && (
                    <EmptyText>{t('api.signature.noInput')}</EmptyText>
                )}
            </Section>
            <Section>
                <SectionTitle>
                    {outputDatasets.length > 0
                        ? t('api.signature.outputDatasetsTitle')
                        : t('api.signature.outputTitle')}
                </SectionTitle>
                {outputDatasets.length > 0 && <DatasetRefs datasets={outputDatasets} />}
                {outputDatasets.length > 0 ? (
                    outputFields.length > 0 && <FieldsTable fields={outputFields} />
                ) : (
                    <OutputSection fields={outputFields} />
                )}
            </Section>
        </TabContent>
    );
}
