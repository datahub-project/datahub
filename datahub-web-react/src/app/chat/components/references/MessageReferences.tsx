import { Button, Loader, Text, colors } from '@components';
import { CaretDown, CaretRight } from '@phosphor-icons/react';
import React, { useMemo, useState } from 'react';
import styled from 'styled-components';

import { ReferenceCard } from '@app/chat/components/references/ReferenceCard';
import { extractUrnsFromMarkdown } from '@app/chat/utils/extractUrnsFromMarkdown';
import { useGetEntities } from '@app/sharedV2/useGetEntities';

import { Entity } from '@types';

const Container = styled.div`
    margin-top: 12px;
    width: 100%;
`;

const Header = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    cursor: pointer;
    padding: 4px 0;
    user-select: none;

    &:hover {
        opacity: 0.8;
    }
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
}

export const MessageReferences: React.FC<Props> = ({ messageText, selectedEntityUrn, onEntitySelect }) => {
    const [isExpanded, setIsExpanded] = useState(false);

    // Extract URNs from markdown links
    const urns = useMemo(() => {
        const extracted = extractUrnsFromMarkdown(messageText);
        return extracted;
    }, [messageText]);

    // Fetch entities
    const { entities, loading } = useGetEntities(urns);

    // Don't render if no URNs found
    if (urns.length === 0) {
        return null;
    }

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

    return (
        <Container>
            <Header onClick={handleToggle}>
                <Button variant="text" size="sm" style={{ padding: 0, minWidth: 'auto' }}>
                    {isExpanded ? (
                        <CaretDown size={16} weight="bold" color={colors.gray[600]} />
                    ) : (
                        <CaretRight size={16} weight="bold" color={colors.gray[600]} />
                    )}
                </Button>
                <Text color="gray" style={{ fontSize: '14px', fontWeight: 500 }}>
                    References ({entities.length})
                </Text>
            </Header>

            {isExpanded && (
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
