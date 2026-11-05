import { Button, Divider, Icon, Pill, TabButtons, Tooltip } from '@components';
import { ArrowDown } from '@phosphor-icons/react/dist/csr/ArrowDown';
import { ArrowUp } from '@phosphor-icons/react/dist/csr/ArrowUp';
import { MagnifyingGlass } from '@phosphor-icons/react/dist/csr/MagnifyingGlass';
import React, { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

import { useEntityData } from '@app/entityV2/shared/EntityContext';
import { useIsSeparateSiblingsMode } from '@app/entityV2/shared/siblingUtils';
import { ImpactAnalysis } from '@app/entityV2/shared/tabs/Lineage/ImpactAnalysis';
import { LineageTabContext } from '@app/entityV2/shared/tabs/Lineage/LineageTabContext';
import { UnionType } from '@app/search/utils/constants';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { LineageDirection, LineageSearchPath } from '@types';

const Container = styled.div`
    flex: 1;
    overflow: hidden;
    display: flex;
    flex-direction: column;
`;

const ThinDivider = styled(Divider)`
    && {
        margin: 0px;
        padding: 0px;
    }
`;

const Actions = styled.div`
    justify-content: space-between;
    z-index: 2;
    padding: 16px 20px;
`;

const Filters = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 12px 20px;
`;

const LevelFilters = styled.div`
    display: flex;
    align-items: center;
    justify-content: start;
    gap: 12px;
`;

const AdvancedFiltersButton = styled(Button)<{ $isSelected: boolean }>`
    && {
        padding: 0px 4px;
        font-size: 16px;
        color: ${(props) => (props.$isSelected ? props.theme.colors.textSelected : props.theme.colors.textTertiary)};
    }
`;

const StyledArrowDownOutlined = styled(ArrowDown)`
    && {
        margin-right: 4px;
        font-size: 10px;
    }
`;

const StyledArrowUpOutlined = styled(ArrowUp)`
    && {
        margin-right: 4px;
        font-size: 10px;
    }
`;

const Results = styled.div`
    flex: 1;
    overflow: hidden;
    display: flex;
    min-height: 0;
    & > div {
        width: 100%;
        height: 100%;
        display: flex;
        flex-direction: column;
    }
`;

enum LevelFilterType {
    DIRECT = 'DIRECT',
    INDIRECT = 'INDIRECT',
}

const DEFAULT_SELECTED_LEVELS = new Set([LevelFilterType.DIRECT]);

export const CompactLineageTab = ({ defaultDirection }: { defaultDirection: LineageDirection }) => {
    const { t } = useTranslation('lineage');
    const entityRegistry = useEntityRegistry();
    const { urn, entityData, entityType } = useEntityData();
    const onIndividualSiblingPage = useIsSeparateSiblingsMode();
    const lineageUrn = (!onIndividualSiblingPage && entityData?.lineageUrn) || urn;
    const [selectedDirection, setDirection] = useState<LineageDirection>(defaultDirection);
    const [selectedLevels, setSelectedLevels] = useState<Set<LevelFilterType>>(DEFAULT_SELECTED_LEVELS);
    const [showAdvancedFilters, setShowAdvancedFilters] = useState(false);
    const [lineageSearchPath, setLineageSearchPath] = useState<LineageSearchPath | null>(null);
    const entityName = (entityData && entityRegistry.getDisplayName(entityType, entityData)) || '-';

    const toggleLevelFilter = (level: LevelFilterType) => {
        if (selectedLevels.has(level)) {
            const newLevels = new Set(selectedLevels);
            newLevels.delete(level);
            setSelectedLevels(newLevels);
        } else {
            const newLevels = new Set(selectedLevels);
            newLevels.add(level);
            setSelectedLevels(newLevels);
        }
    };

    const directionOptions = [
        {
            label: (
                <span data-testid="compact-lineage-tab-direction-select-option-upstream">
                    <StyledArrowUpOutlined />
                    <b>{t('direction.upstreams')}</b>
                </span>
            ),
            value: LineageDirection.Upstream,
            tip: t('direction.upstreamTooltip', { entityName }),
        },
        {
            label: (
                <span data-testid="compact-lineage-tab-direction-select-option-downstream">
                    <StyledArrowDownOutlined />
                    <b>{t('direction.downstreams')}</b>
                </span>
            ),
            value: LineageDirection.Downstream,
            tip: t('direction.downstreamTooltip', { entityName }),
        },
    ];

    useEffect(() => {
        setSelectedLevels(DEFAULT_SELECTED_LEVELS);
    }, [selectedDirection]);

    const getLevelFilterSet = (levels: Set<LevelFilterType>) => {
        const levelFilters: string[] = [];
        if (selectedLevels.size > 0) {
            if (levels.has(LevelFilterType.DIRECT)) {
                levelFilters.push('1');
            }
            if (levels.has(LevelFilterType.INDIRECT)) {
                levelFilters.push('2');
                levelFilters.push('3+');
            }
            return {
                unionType: UnionType.OR,
                filters: [
                    {
                        field: 'degree',
                        values: levelFilters,
                    },
                ],
            };
        }
        return undefined;
    };

    const levelFilters = getLevelFilterSet(selectedLevels);

    return (
        <Container>
            <Actions>
                <TabButtons
                    tabs={directionOptions.map((option) => ({
                        key: option.value,
                        label: (
                            <Tooltip title={option.tip} placement="bottom" showArrow={false}>
                                {option.label}
                            </Tooltip>
                        ),
                    }))}
                    activeTab={selectedDirection}
                    onTabClick={(key) => setDirection(key as LineageDirection)}
                />
            </Actions>
            <ThinDivider />
            <Filters>
                <LevelFilters>
                    <Tooltip title={t('levelFilter.directTooltip')} placement="bottom" showArrow={false}>
                        <span>
                            <Pill
                                label={t('levelFilter.direct')}
                                variant="outline"
                                color={selectedLevels.has(LevelFilterType.DIRECT) ? 'primary' : 'gray'}
                                clickable
                                onPillClick={() => toggleLevelFilter(LevelFilterType.DIRECT)}
                            />
                        </span>
                    </Tooltip>
                    <Tooltip title={t('levelFilter.indirectTooltip')} placement="bottom" showArrow={false}>
                        <span>
                            <Pill
                                label={t('levelFilter.indirect')}
                                variant="outline"
                                color={selectedLevels.has(LevelFilterType.INDIRECT) ? 'primary' : 'gray'}
                                clickable
                                onPillClick={() => toggleLevelFilter(LevelFilterType.INDIRECT)}
                            />
                        </span>
                    </Tooltip>
                </LevelFilters>
                <Tooltip
                    placement="left"
                    title={!showAdvancedFilters ? t('searchBar.showTooltip') : t('searchBar.hideTooltip')}
                    showArrow={false}
                >
                    <AdvancedFiltersButton
                        variant="link"
                        $isSelected={showAdvancedFilters}
                        onClick={() => setShowAdvancedFilters(!showAdvancedFilters)}
                    >
                        <Icon icon={MagnifyingGlass} />
                    </AdvancedFiltersButton>
                </Tooltip>
            </Filters>
            <ThinDivider />
            <Results>
                <LineageTabContext.Provider
                    value={{
                        isColumnLevelLineage: false,
                        lineageDirection: selectedDirection,
                        lineageSearchPath,
                        setLineageSearchPath,
                    }}
                >
                    <ImpactAnalysis
                        type="compact"
                        urn={lineageUrn}
                        direction={selectedDirection as LineageDirection}
                        skipCache={false}
                        defaultShowFilters={false}
                        defaultFilters={[]}
                        showFilterBar={showAdvancedFilters}
                        fixedFilters={levelFilters}
                    />
                </LineageTabContext.Provider>
            </Results>
        </Container>
    );
};
