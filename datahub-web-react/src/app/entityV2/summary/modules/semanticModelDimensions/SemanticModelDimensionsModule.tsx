import { Pill, Text, Tooltip } from '@components';
import { Clock } from '@phosphor-icons/react/dist/csr/Clock';
import { Cube } from '@phosphor-icons/react/dist/csr/Cube';
import { Function } from '@phosphor-icons/react/dist/csr/Function';
import React, { useCallback, useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { ColorOptions } from '@components/theme/config';

import analytics, { EventType } from '@app/analytics';
import { useAllSemanticModelMemberDatasets } from '@app/entityV2/summary/modules/semanticModelDatasets/useSemanticModelMemberDatasets';
import {
    DimensionPillKind,
    getDimensionGroups,
    getDimensionPillKind,
} from '@app/entityV2/summary/modules/semanticModelDimensions/utils';
import {
    getSemanticModelDatasetDisplayName,
    getSemanticModelDatasetLabel,
} from '@app/entityV2/summary/modules/shared/semanticModelDatasetUtils';
import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';
import EmptyContent from '@app/homeV3/module/components/EmptyContent';
import LargeModule from '@app/homeV3/module/components/LargeModule';
import ModuleEntityIconSlot from '@app/homeV3/module/components/ModuleEntityIconSlot';
import ModuleEntityLink from '@app/homeV3/module/components/ModuleEntityLink';
import ModuleEntityName from '@app/homeV3/module/components/ModuleEntityName';
import ModulePillRow from '@app/homeV3/module/components/ModulePillRow';
import ModuleSecondaryText from '@app/homeV3/module/components/ModuleSecondaryText';
import { ModuleProps } from '@app/homeV3/module/types';
import EntityIcon from '@app/searchV2/autoCompleteV2/components/icon/EntityIcon';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { DataHubPageModuleType, Dataset } from '@types';

const DIMENSION_PILL_UI: Record<DimensionPillKind, { color: ColorOptions; leftIcon?: React.ComponentType<any> }> = {
    time: { color: 'blue', leftIcon: Clock },
    calculated: { color: 'yellow', leftIcon: Function },
    plain: { color: 'gray' },
};

const DatasetGroup = styled.div`
    display: flex;
    flex-direction: row;
    align-items: center;
    gap: 16px;
    padding: 8px 13px 8px 8px;
`;

const GroupBody = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;
    min-width: 0;
    flex: 1;
`;

const HeaderRow = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 8px;
    min-width: 0;
`;

const HeaderLink = styled(ModuleEntityLink)`
    display: flex;
    align-items: center;
    gap: 16px;
    min-width: 0;
    flex: 1;
    overflow: hidden;
`;

const DatasetName = styled(ModuleEntityName)`
    ${HeaderLink}:hover & {
        text-decoration: underline;
    }
`;

export default function SemanticModelDimensionsModule(props: ModuleProps) {
    const { t } = useTranslation('modules');
    const entityRegistry = useEntityRegistryV2();
    const { templateType } = usePageTemplateContext();
    const { datasets, loading } = useAllSemanticModelMemberDatasets();

    const groups = useMemo(() => getDimensionGroups(datasets), [datasets]);

    const onDatasetClick = useCallback(
        (datasetUrn: string) => {
            analytics.event({
                type: EventType.HomePageTemplateModuleAssetClick,
                moduleType: DataHubPageModuleType.SemanticModelDimensions,
                assetUrn: datasetUrn,
                location: templateType,
            });
        },
        [templateType],
    );

    const renderHover = useCallback((dataset: Dataset, children: React.ReactNode) => {
        const alias = dataset.semanticModelProperties?.alias;
        const name = getSemanticModelDatasetDisplayName(dataset);
        const hoverText = alias ? `${alias} - ${name}` : name;
        return (
            <Tooltip title={<Text size="sm">{hoverText}</Text>} placement="bottom">
                {children}
            </Tooltip>
        );
    }, []);

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
                const label = getSemanticModelDatasetLabel(group.dataset);
                const typeName = entityRegistry.getEntityName(group.dataset.type) ?? group.dataset.type;
                const datasetUrl = entityRegistry.getEntityUrl(group.dataset.type, group.dataset.urn);

                return (
                    <DatasetGroup key={group.dataset.urn}>
                        <GroupBody>
                            <HeaderRow>
                                <HeaderLink to={datasetUrl} onClick={() => onDatasetClick(group.dataset.urn)}>
                                    <ModuleEntityIconSlot>
                                        <EntityIcon entity={group.dataset} />
                                    </ModuleEntityIconSlot>
                                    {renderHover(group.dataset, <DatasetName displayName={label} />)}
                                </HeaderLink>
                                <ModuleSecondaryText>{typeName}</ModuleSecondaryText>
                            </HeaderRow>
                            <ModulePillRow>
                                {group.fields.map((field) => {
                                    const fieldPath = field.fieldPath ?? '';
                                    const { color, leftIcon } = DIMENSION_PILL_UI[getDimensionPillKind(field)];
                                    return (
                                        <Pill
                                            key={`${group.dataset.urn}-${fieldPath}`}
                                            label={fieldPath}
                                            color={color}
                                            size="sm"
                                            leftIcon={leftIcon}
                                            clickable={false}
                                        />
                                    );
                                })}
                            </ModulePillRow>
                        </GroupBody>
                    </DatasetGroup>
                );
            })}
        </LargeModule>
    );
}
