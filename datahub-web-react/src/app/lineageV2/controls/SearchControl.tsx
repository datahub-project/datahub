import { SearchOutlined } from '@ant-design/icons';
import { ANTD_GRAY, REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { LineageDisplayContext, LineageNodesContext } from '@app/lineageV2/common';
import LineageVisualizationContext from '@app/lineageV2/LineageVisualizationContext';
import { Button } from '@components';
import { Input, InputRef } from 'antd';
import React, { useCallback, useContext, useEffect, useMemo, useRef, useState } from 'react';
import { useDebounce } from 'react-use';
import { Panel } from 'reactflow';
import styled from 'styled-components';

const StyledPanel = styled(Panel)`
    margin-top: 20px;
`;

const StyledInput = styled(Input)<{ width: number }>`
    min-width: 50px;
    min-height: 50px;
    width: ${({ width }) => width}px;

    display: flex;
    align-items: center;
    justify-content: center;

    font-size: 14px;

    border-color: ${ANTD_GRAY[5]} !important;
    box-shadow: none !important;
`;

const ClosedSearchIcon = styled(SearchOutlined)`
    margin-left: 5px;
    font-size: 16px;
`;

const OpenSearchIcon = styled(SearchOutlined)`
    color: ${REDESIGN_COLORS.PLACEHOLDER_PURPLE};
`;

const VerticalDivider = styled.hr<{ margin: number }>`
    align-self: stretch;
    height: auto;
    margin: 0 ${({ margin }) => margin}px;
    border: 0.5px solid ${ANTD_GRAY[5]};
    vertical-align: text-top;
`;

export default function SearchControl() {
    const { searchQuery, setSearchQuery, setSearchedEntity } = useContext(LineageVisualizationContext);
    const [isFocused, setIsFocused] = useState(false);

    const inputRef = useRef<InputRef>(null);
    useCaptureKeyboardSearch(inputRef, setIsFocused);

    const matchedNodes = useComputeMatchedNodes();
    const searchIndex = useAssignSearchedEntity(matchedNodes);

    const prev = useCallback(
        () => setSearchedEntity(matchedNodes[(searchIndex - 1 + matchedNodes.length) % matchedNodes.length]),
        [matchedNodes, searchIndex, setSearchedEntity],
    );
    const next = useCallback(
        () => setSearchedEntity(matchedNodes[(searchIndex + 1) % matchedNodes.length]),
        [matchedNodes, searchIndex, setSearchedEntity],
    );

    const isOpen = isFocused || !!searchQuery;
    const close = useCallback(() => {
        setSearchQuery('');
        setIsFocused(false);
        setTimeout(() => inputRef.current?.blur(), 0);
    }, [setSearchQuery]);
    useCaptureEscape(isOpen, close);

    return (
        <StyledPanel position="top-left">
            <StyledInput
                ref={inputRef}
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                onPressEnter={(e) => {
                    if (e.shiftKey) prev();
                    else next();
                }}
                placeholder={isOpen ? 'Search Graph' : undefined}
                width={isOpen ? 330 : 40}
                prefix={isOpen ? <OpenSearchIcon /> : <ClosedSearchIcon />}
                suffix={
                    isOpen &&
                    searchQuery && (
                        <>
                            <span>
                                {matchedNodes.length ? searchIndex + 1 : 0} / {matchedNodes.length}
                            </span>
                            <VerticalDivider margin={8} />
                            <Button icon="KeyboardArrowUp" variant="outline" size="sm" onClick={prev} />
                            <Button icon="KeyboardArrowDown" variant="outline" size="sm" onClick={next} />
                            <Button icon="Close" variant="outline" size="sm" onClick={close} />
                        </>
                    )
                }
                onBlur={() => setIsFocused(false)}
                onFocus={() => setIsFocused(true)}
            />
        </StyledPanel>
    );
}

function useComputeMatchedNodes() {
    const { nodes } = useContext(LineageNodesContext);
    const { shownUrns } = useContext(LineageDisplayContext);
    const { searchQuery } = useContext(LineageVisualizationContext);

    return useMemo(() => {
        if (!searchQuery) return [];

        return shownUrns.filter((urn) => {
            const entity = nodes.get(urn)?.entity;
            return (
                entity?.name.toLowerCase()?.includes(searchQuery.toLowerCase()) ||
                entity?.parent?.name?.toLowerCase().includes(searchQuery.toLowerCase())
            );
        });
    }, [searchQuery, shownUrns, nodes]);
}

function useAssignSearchedEntity(matchedNodes: string[]) {
    const { searchQuery, searchedEntity, setSearchedEntity } = useContext(LineageVisualizationContext);

    const [searchIndex, newSearchedEntity] = useMemo(() => {
        if (!searchedEntity) {
            // No previously selected entity, default to first matched node
            return [0, matchedNodes.length ? matchedNodes[0] : null];
        }
        const index = matchedNodes.indexOf(searchedEntity);
        if (index === -1) {
            // Previously selected entity no longer in search list, default to first matched node
            return [0, matchedNodes[0]];
        }
        // Previously selected entity still in search list, keep it selected and recalculate index
        return [index, searchedEntity];
    }, [searchedEntity, matchedNodes]);

    // Add debounce so graph doesn't jump around while user is typing
    useDebounce(() => setSearchedEntity(newSearchedEntity), 300, [searchQuery]);

    return searchIndex;
}

function useCaptureKeyboardSearch(inputRef: React.RefObject<InputRef>, setIsFocused: (value: boolean) => void) {
    const { isFocused } = useContext(LineageVisualizationContext);

    const handleKeyPress = useCallback(
        (e: KeyboardEvent) => {
            // Capture ctrl-f or cmd-f
            if ((e.ctrlKey || e.metaKey) && e.key === 'f') {
                e.preventDefault();
                inputRef.current?.focus();
                setIsFocused(true);
            }
        },
        [inputRef, setIsFocused],
    );

    useEffect(() => {
        if (isFocused) {
            document.addEventListener('keydown', handleKeyPress);
            return () => {
                document.removeEventListener('keydown', handleKeyPress);
            };
        }
        return () => {};
    }, [handleKeyPress, isFocused]);
}

function useCaptureEscape(isOpen: boolean, close: () => void) {
    const { isFocused } = useContext(LineageVisualizationContext);

    const handleKeyPress = useCallback(
        (e: KeyboardEvent) => {
            if (e.key === 'Escape') {
                close();
            }
        },
        [close],
    );

    useEffect(() => {
        if (isFocused && isOpen) {
            document.addEventListener('keydown', handleKeyPress);
            return () => {
                document.removeEventListener('keydown', handleKeyPress);
            };
        }
        return () => {};
    }, [isOpen, handleKeyPress, isFocused]);
}
