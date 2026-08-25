import { Pill, Text, radius } from '@components';
import { Clock } from '@phosphor-icons/react/dist/csr/Clock';
import { Cube } from '@phosphor-icons/react/dist/csr/Cube';
import { Function } from '@phosphor-icons/react/dist/csr/Function';
import React, { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { ColorOptions } from '@components/theme/config';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { useAllSemanticModelMemberDatasets } from '@app/entityV2/summary/modules/semanticModelDatasets/useSemanticModelMemberDatasets';
import EmptyContent from '@app/homeV3/module/components/EmptyContent';
import LargeModule from '@app/homeV3/module/components/LargeModule';
import { ModuleProps } from '@app/homeV3/module/types';
import { PlatformIcon } from '@app/searchV2/autoCompleteV2/components/icon/PlatformIcon';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { DataPlatform, Dataset, EntityType, SchemaField, SemanticFieldType } from '@types';

type EntityDataWithPlatform = {
    platform?: DataPlatform | null;
};

type DimensionGroup = {
    datasetName: string;
    platform?: DataPlatform | null;
    source?: Dataset | null;
    fields: SchemaField[];
};

const DatasetGroup = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
    padding: 10px 4px;
    border-bottom: 1px solid ${(props) => props.theme.colors.border};

    &:last-child {
        border-bottom: none;
    }
`;

const DatasetHeader = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    min-width: 0;
`;

const IconContainer = styled.div`
    display: flex;
    justify-content: center;
    align-items: center;
    background: ${(props) => props.theme.colors.bgSurface};
    height: 28px;
    width: 28px;
    border-radius: ${radius.full};
    flex-shrink: 0;
`;

const DatasetName = styled(Text).attrs({ size: 'sm', weight: 'semiBold' })`
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

const PillsRow = styled.div`
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
`;

const StyledLink = styled(Link)`
    color: inherit;
    text-decoration: none;

    &:hover ${DatasetName} {
        text-decoration: underline;
    }
`;

const PLATFORM_ICON_SIZE = 16;

function isCalculatedDimension(field: SchemaField): boolean {
    const fieldPath = field.fieldPath ?? '';
    const annotation = field.schemaFieldEntity?.semanticFieldAnnotation;
    return (annotation?.expression?.dialects ?? []).some((dialect) => {
        const expression = (dialect.expression ?? '').trim();
        if (!expression) {
            return false;
        }
        return expression.toLowerCase() !== fieldPath.toLowerCase();
    });
}

function getDimensionPillProps(field: SchemaField): {
    color: ColorOptions;
    leftIcon?: React.ComponentType<any>;
} {
    if (field.schemaFieldEntity?.semanticFieldAnnotation?.dimension?.isTime) {
        return { color: 'blue', leftIcon: Clock };
    }
    if (isCalculatedDimension(field)) {
        return { color: 'yellow', leftIcon: Function };
    }
    return { color: 'gray' };
}

export default function SemanticModelDimensionsModule(props: ModuleProps) {
    const { t } = useTranslation('modules');
    const { entityData } = useEntityData();
    const entityRegistry = useEntityRegistryV2();
    const { datasets, loading } = useAllSemanticModelMemberDatasets();

    const typedData = entityData as EntityDataWithPlatform | null;
    const fallbackPlatform = typedData?.platform;

    const groups: DimensionGroup[] = useMemo(() => {
        return datasets
            .map((dataset): DimensionGroup | null => {
                const fields = (dataset.schema?.fields ?? []).filter(
                    (field) => field.schemaFieldEntity?.semanticFieldAnnotation?.type === SemanticFieldType.Dimension,
                );
                if (!fields.length) {
                    return null;
                }
                return {
                    datasetName: dataset.semanticModelProperties?.alias || dataset.name,
                    platform: dataset.platform ?? fallbackPlatform,
                    source: dataset,
                    fields,
                };
            })
            .filter((group): group is DimensionGroup => group !== null);
    }, [datasets, fallbackPlatform]);

    if (!loading && !groups.length) {
        return (
            <LargeModule {...props} loading={loading} dataTestId="semantic-model-dimensions-module">
                <EmptyContent
                    icon={Cube}
                    title={t('semanticModelDimensions.emptyTitle')}
                    description={t('semanticModelDimensions.emptyDescription')}
                />
            </LargeModule>
        );
    }

    return (
        <LargeModule {...props} loading={loading} dataTestId="semantic-model-dimensions-module">
            {groups.map((group) => {
                const headerContent = (
                    <DatasetHeader>
                        {group.platform && (
                            <IconContainer>
                                <PlatformIcon platform={group.platform} size={PLATFORM_ICON_SIZE} />
                            </IconContainer>
                        )}
                        <DatasetName>{group.datasetName}</DatasetName>
                    </DatasetHeader>
                );

                return (
                    <DatasetGroup key={group.datasetName}>
                        {group.source?.urn && group.source.type === EntityType.Dataset ? (
                            <StyledLink to={entityRegistry.getEntityUrl(group.source.type, group.source.urn)}>
                                {headerContent}
                            </StyledLink>
                        ) : (
                            headerContent
                        )}
                        <PillsRow>
                            {group.fields.map((field) => {
                                const fieldPath = field.fieldPath ?? '';
                                const { color, leftIcon } = getDimensionPillProps(field);
                                return (
                                    <Pill
                                        key={`${group.datasetName}-${fieldPath}`}
                                        label={fieldPath}
                                        color={color}
                                        size="sm"
                                        leftIcon={leftIcon}
                                    />
                                );
                            })}
                        </PillsRow>
                    </DatasetGroup>
                );
            })}
        </LargeModule>
    );
}
