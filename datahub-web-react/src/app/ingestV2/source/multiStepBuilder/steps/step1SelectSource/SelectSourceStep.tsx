import { Badge, Icon, SearchBar, colors } from '@components';
import React, { useRef, useState } from 'react';
import styled from 'styled-components';

import sourcesJson from '@app/ingestV2/source/builder/sources.json';
import { SourceBuilderState, SourceConfig } from '@app/ingestV2/source/builder/types';
import EmptySearchResults from '@app/ingestV2/source/multiStepBuilder/steps/step1SelectSource/EmptySearchResults';
import ShowAllCard from '@app/ingestV2/source/multiStepBuilder/steps/step1SelectSource/ShowAllCard';
import SourcePlatformCard from '@app/ingestV2/source/multiStepBuilder/steps/step1SelectSource/SourcePlatformCard';
import { useCardsPerRow } from '@app/ingestV2/source/multiStepBuilder/steps/step1SelectSource/useCardsPerRow';
import {
    CARD_WIDTH,
    computeRows,
    groupByCategory,
    sortByPopularFirst,
} from '@app/ingestV2/source/multiStepBuilder/steps/step1SelectSource/utils';
import { IngestionSourceFormStep } from '@app/ingestV2/source/multiStepBuilder/types';
import { useMultiStepContext } from '@app/sharedV2/forms/multiStepForm/MultiStepFormContext';

const StepContainer = styled.div`
    padding: 0 20px 20px 20px;
    display: flex;
    flex-direction: column;
    gap: 16px;
    height: 100%;
`;

const CardsContainer = styled.div`
    padding-bottom: 20px;
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

const Section = styled.div`
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

const SectionHeader = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    font-size: 16px;
    font-weight: 700;
    color: ${colors.gray[600]};
`;

const CardsWrapper = styled.div`
    display: grid;
    grid-template-columns: repeat(auto-fit, ${CARD_WIDTH}px);
    justify-content: start;
    gap: 8px;
`;

const LeftSection = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

const RightSection = styled.div`
    display: flex;
    cursor: pointer;
`;

export function SelectSourceStep() {
    const { updateState, setCurrentStepCompleted, isCurrentStepCompleted, goToNext } = useMultiStepContext<
        SourceBuilderState,
        IngestionSourceFormStep
    >();
    const [searchQuery, setSearchQuery] = useState<string>('');

    const ingestionSources: SourceConfig[] = JSON.parse(JSON.stringify(sourcesJson));
    const filteredSources = ingestionSources.filter((src) =>
        src.displayName.toLowerCase().includes(searchQuery.toLowerCase()),
    );

    const categories = groupByCategory(filteredSources);
    const [expanded, setExpanded] = useState({});

    const containerRef = useRef<HTMLDivElement | null>(null);
    const cardsPerRow = useCardsPerRow(containerRef, 318, 8, 1);

    const [showAllByCategory, setShowAllByCategory] = useState<Record<string, boolean>>({});

    const onSelectCard = (platformName: string) => {
        if (!isCurrentStepCompleted()) {
            setCurrentStepCompleted();
        }
        updateState({
            type: platformName,
        });
        goToNext();
    };

    const handleSearch = (value: string) => {
        setSearchQuery(value);
    };

    return (
        <StepContainer ref={containerRef}>
            <SearchBar
                placeholder="Search..."
                value={searchQuery}
                onChange={(value) => handleSearch(value)}
                width="320px"
            />
            {searchQuery && filteredSources.length === 0 ? (
                <EmptySearchResults />
            ) : (
                <CardsContainer>
                    {Object.entries(categories)
                        .sort(([a], [b]) => {
                            if (a === 'Other') return 1;
                            if (b === 'Other') return -1;
                            return a.localeCompare(b);
                        })
                        .map(([category, list]) => {
                            const sorted = sortByPopularFirst(list);
                            const popular = sorted.filter((s) => s.isPopular);
                            const nonPopular = sorted.filter((s) => !s.isPopular);

                            const { visible: computedVisible, hidden: computedHidden } = computeRows(
                                popular,
                                nonPopular,
                                cardsPerRow,
                            );

                            const isOpen = expanded[category] ?? true;
                            const showAll = showAllByCategory[category] ?? false;

                            const visible = showAll || searchQuery ? [...popular, ...nonPopular] : computedVisible;
                            const hidden = showAll || searchQuery ? [] : computedHidden;

                            return (
                                <Section key={category}>
                                    <SectionHeader>
                                        <LeftSection>
                                            {category}
                                            <Badge count={list.length} size="xs" />
                                        </LeftSection>

                                        {!searchQuery && (
                                            <RightSection>
                                                <Icon
                                                    icon={isOpen ? 'CaretDown' : 'CaretRight'}
                                                    source="phosphor"
                                                    color="gray"
                                                    size="2xl"
                                                    onClick={() =>
                                                        setExpanded((prev) => ({ ...prev, [category]: !isOpen }))
                                                    }
                                                />
                                            </RightSection>
                                        )}
                                    </SectionHeader>

                                    {isOpen && (
                                        <div>
                                            <CardsWrapper>
                                                {visible.map((src) => (
                                                    <SourcePlatformCard
                                                        key={src.urn || src.name}
                                                        source={src}
                                                        onSelect={onSelectCard}
                                                    />
                                                ))}

                                                {!searchQuery && !showAll && hidden.length > 0 && (
                                                    <ShowAllCard
                                                        hiddenSources={hidden}
                                                        onShowAll={() =>
                                                            setShowAllByCategory((prev) => ({
                                                                ...prev,
                                                                [category]: true,
                                                            }))
                                                        }
                                                    />
                                                )}
                                            </CardsWrapper>
                                        </div>
                                    )}
                                </Section>
                            );
                        })}
                </CardsContainer>
            )}
        </StepContainer>
    );
}
