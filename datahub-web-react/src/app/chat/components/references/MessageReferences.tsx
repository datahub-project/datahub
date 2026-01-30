import { Loader, colors } from '@components';
import { CaretDown, CaretRight } from '@phosphor-icons/react';
import React, { useMemo, useState } from 'react';
import styled from 'styled-components';

import { ReferenceCard } from '@app/chat/components/references/ReferenceCard';
import { StackedEntityLogos } from '@app/chat/components/references/StackedEntityLogos';
import { extractUrnsFromMarkdown } from '@app/chat/utils/extractUrnsFromMarkdown';
import { useGetEntities } from '@app/sharedV2/useGetEntities';

import { Entity } from '@types';

const ReferencesLabel = styled.span`
    font-size: 14px;
    font-weight: 500;
    color: ${colors.gray[1700]};
`;

const CaretIcon = styled.span`
    display: flex;
    align-items: center;
    color: ${colors.gray[1800]};
`;

const Container = styled.div`
    width: 100%;
`;

const HeaderRow = styled.div<{ $hasSources: boolean }>`
    display: flex;
    align-items: center;
    justify-content: flex-start;
    gap: 2px;
    width: 100%;
    min-height: 32px;
`;

const SourcesToggle = styled.div`
    display: flex;
    align-items: center;
    gap: 4px;
    cursor: pointer;
    padding: 4px 8px;
    border-radius: 4px;
    user-select: none;

    &:hover {
        background-color: ${colors.gray[1500]};
    }
`;

const RightContent = styled.div`
    display: flex;
    align-items: center;
    gap: 2px;
`;

const ReferencesList = styled.div`
    display: flex;
    flex-direction: column;
    gap: 12px;
    margin-top: 12px;
    width: 100%;
`;

const LoadingContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 20px;
`;

interface Props {
    messageText: string;
    selectedEntityUrn?: string;
    onEntitySelect: (entity: Entity | null) => void;
    /** Optional content to render on the right side of the header (e.g., reaction buttons) */
    rightContent?: React.ReactNode;
}

export const MessageReferences: React.FC<Props> = ({
    messageText,
    selectedEntityUrn,
    onEntitySelect,
    rightContent,
}) => {
    const [isExpanded, setIsExpanded] = useState(false);

    // Extract URNs from markdown links
    const urns = useMemo(() => extractUrnsFromMarkdown(messageText), [messageText]);

    // Fetch entities
    const { entities, loading } = useGetEntities(urns);

    // Show Sources when loading (URNs exist) or when entities are loaded
    // Hide if loading finishes but no valid entities were found
    const hasSources = urns.length > 0 && (loading || entities.length > 0);

    const handleToggle = () => {
        setIsExpanded(!isExpanded);
    };

    const handleEntityClick = (entity: Entity) => {
        // If clicking the same entity, deselect it
        if (selectedEntityUrn === entity.urn) {
            onEntitySelect(null);
        } else {
            onEntitySelect(entity);
        }
    };

    // If no sources and no right content, don't render anything
    if (!hasSources && !rightContent) {
        return null;
    }

    return (
        <Container>
            <HeaderRow $hasSources={hasSources}>
                {rightContent && <RightContent>{rightContent}</RightContent>}
                {hasSources && (
                    <SourcesToggle onClick={handleToggle}>
                        <ReferencesLabel>Sources</ReferencesLabel>
                        {loading ? (
                            <Loader size="xs" />
                        ) : (
                            entities.length > 0 && <StackedEntityLogos entities={entities} />
                        )}
                        <CaretIcon>
                            {isExpanded ? (
                                <CaretDown size={14} weight="bold" />
                            ) : (
                                <CaretRight size={14} weight="bold" />
                            )}
                        </CaretIcon>
                    </SourcesToggle>
                )}
            </HeaderRow>

            {hasSources && isExpanded && (
                <>
                    {loading && (
                        <LoadingContainer>
                            <Loader size="md" />
                        </LoadingContainer>
                    )}
                    {!loading && entities.length > 0 && (
                        <ReferencesList>
                            {entities.map((entity) => (
                                <ReferenceCard
                                    key={entity.urn}
                                    entity={entity}
                                    isSelected={selectedEntityUrn === entity.urn}
                                    onClick={handleEntityClick}
                                />
                            ))}
                        </ReferencesList>
                    )}
                </>
            )}
        </Container>
    );
};
