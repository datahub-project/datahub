import { ArrowDownOutlined, ArrowUpOutlined, SearchOutlined } from '@ant-design/icons';
import { useIsSeparateSiblingsMode } from '@app/entity/shared/siblingUtils';
import { Button, Divider } from 'antd';
import { Tooltip } from '@components';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components/macro';
import { LineageDirection } from '../../../../../types.generated';
import { UnionType } from '../../../../search/utils/constants';
import { useEntityRegistry } from '../../../../useEntityRegistry';
import { useEntityData } from '../../../../entity/shared/EntityContext';
import { ANTD_GRAY, SEARCH_COLORS } from '../../constants';
import { ImpactAnalysis } from './ImpactAnalysis';
import { LineageTabContext } from './LineageTabContext';

// Unfortunately, we have to artificially bound the height.
// Accounts for search bar, controls header, and padding.
const Container = styled.div`
    flex: 1;
    overflow: hidden;
    display: flex;
    flex-direction: column;
`;

const LineageButton = styled(Button)<{ $isSelected: boolean }>`
    &&& {
        background-color: ${(props) => (props.$isSelected ? SEARCH_COLORS.TITLE_PURPLE : 'none')};
        color: ${(props) => (props.$isSelected ? '#ffffff' : ANTD_GRAY[7])};
        border-radius: 8px;
        margin-right: 12px;
        min-height: 32px;
        ${(props) => props.$isSelected && `border-color: ${SEARCH_COLORS.TITLE_PURPLE}`};
    }
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
`;

const LevelFilter = styled.div<{ $isSelected: boolean }>`
    font-size: 14px;
    padding: 2px 8px;
    margin-right: 12px;
    border-radius: 8px;
    color: ${(props) => (props.$isSelected ? SEARCH_COLORS.TITLE_PURPLE : ANTD_GRAY[7])};
    border: 1px solid ${(props) => (props.$isSelected ? SEARCH_COLORS.TITLE_PURPLE : ANTD_GRAY[7])};
    &:hover {
        opacity: 0.8;
        cursor: pointer;
    }
`;

const AdvancedFiltersButton = styled(Button)<{ $isSelected: boolean }>`
    && {
        padding: 0px 4px;
        font-size: 16px;
        color: ${(props) => (props.$isSelected ? '#00615F' : ANTD_GRAY[7])};
    }
`;

const StyledArrowDownOutlined = styled(ArrowDownOutlined)`
    && {
        margin-right: 4px;
        font-size: 10px;
    }
`;

const StyledArrowUpOutlined = styled(ArrowUpOutlined)`
    && {
        margin-right: 4px;
        font-size: 10px;
    }
`;

const Results = styled.div`
    flex: 1;
    overflow: auto;
    display: flex;
    & > div {
        width: 100%;
    }
`;

enum LevelFilterType {
    DIRECT = 'DIRECT',
    INDIRECT = 'INDIRECT',
}

const DEFAULT_SELECTED_LEVELS = new Set([LevelFilterType.DIRECT]);

export const CompactLineageTab = ({ defaultDirection }: { defaultDirection: LineageDirection }) => {
    const entityRegistry = useEntityRegistry();
    const { urn, entityData, entityType } = useEntityData();
    const onIndividualSiblingPage = useIsSeparateSiblingsMode();
    const lineageUrn = (!onIndividualSiblingPage && entityData?.lineageUrn) || urn;
    const [selectedDirection, setDirection] = useState<LineageDirection>(defaultDirection);
    const [selectedLevels, setSelectedLevels] = useState<Set<LevelFilterType>>(DEFAULT_SELECTED_LEVELS);
    const [showAdvancedFilters, setShowAdvancedFilters] = useState(false);
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
                    <b>Upstreams</b>
                </span>
            ),
            value: LineageDirection.Upstream,
            tip: `View the data assets that ${entityName} depends on`,
        },
        {
            label: (
                <span data-testid="compact-lineage-tab-direction-select-option-downstream">
                    <StyledArrowDownOutlined />
                    <b>Downstreams</b>
                </span>
            ),
            value: LineageDirection.Downstream,
            tip: `View the data assets that depend on ${entityName}`,
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
                {directionOptions.map((option) => (
                    <Tooltip title={option.tip} placement="bottom" showArrow={false}>
                        <LineageButton
                            $isSelected={selectedDirection === option.value}
                            onClick={() => setDirection(option.value)}
                        >
                            {option.label}
                        </LineageButton>
                    </Tooltip>
                ))}
            </Actions>
            <ThinDivider />
            <Filters>
                <LevelFilters>
                    <Tooltip title="Show directly related data assets" placement="bottom" showArrow={false}>
                        <LevelFilter
                            $isSelected={selectedLevels.has(LevelFilterType.DIRECT)}
                            onClick={() => toggleLevelFilter(LevelFilterType.DIRECT)}
                        >
                            direct
                        </LevelFilter>
                    </Tooltip>
                    <Tooltip title="Show indirectly related data assets" placement="bottom" showArrow={false}>
                        <LevelFilter
                            $isSelected={selectedLevels.has(LevelFilterType.INDIRECT)}
                            onClick={() => toggleLevelFilter(LevelFilterType.INDIRECT)}
                        >
                            indirect
                        </LevelFilter>
                    </Tooltip>
                </LevelFilters>
                <Tooltip
                    placement="left"
                    title={!showAdvancedFilters ? 'Show search bar' : 'Hide search bar'}
                    showArrow={false}
                >
                    <AdvancedFiltersButton
                        type="link"
                        $isSelected={showAdvancedFilters}
                        onClick={() => setShowAdvancedFilters(!showAdvancedFilters)}
                    >
                        <SearchOutlined />
                    </AdvancedFiltersButton>
                </Tooltip>
            </Filters>
            <ThinDivider />
            <Results>
                <LineageTabContext.Provider
                    value={{ isColumnLevelLineage: false, lineageDirection: selectedDirection }}
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
