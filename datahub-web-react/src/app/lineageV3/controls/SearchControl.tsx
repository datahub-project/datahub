import { Button, SearchBar } from '@components';
import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import { CaretUp } from '@phosphor-icons/react/dist/csr/CaretUp';
import { MagnifyingGlass } from '@phosphor-icons/react/dist/csr/MagnifyingGlass';
import { X } from '@phosphor-icons/react/dist/csr/X';
import { InputRef } from 'antd';
import React, { useCallback, useContext, useEffect, useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useDebounce } from 'react-use';
import { Panel } from 'reactflow';
import styled from 'styled-components';

import { InputContainer } from '@components/components/Input/components';

import LineageControlIcon from '@app/lineage/controls/LineageControlIcon';
import { LINEAGE_CONTROL_ICON_WEIGHT } from '@app/lineage/controls/constants';
import { getWrappedIndex } from '@app/lineage/controls/lineageControlsUtils';
import LineageVisualizationContext from '@app/lineageV3/LineageVisualizationContext';
import { LineageDisplayContext, LineageNodesContext } from '@app/lineageV3/common';

const COLLAPSED_SIZE = 36;
const OPEN_WIDTH = 300;
const SEARCH_BAR_HEIGHT = '24px';

const StyledPanel = styled(Panel)`
    margin-top: 20px;
`;

const SearchRow = styled.div<{ $width: number }>`
    display: flex;
    align-items: center;
    gap: 4px;
    width: ${({ $width }) => $width}px;
`;

const CollapsedSearch = styled(InputContainer)`
    min-width: ${COLLAPSED_SIZE}px;
    min-height: ${COLLAPSED_SIZE}px;
    height: ${COLLAPSED_SIZE}px;
    width: ${COLLAPSED_SIZE}px;
    padding: 0;
    justify-content: center;
    cursor: text;
`;

const StyledSearchBar = styled(SearchBar)`
    flex: 1;
    min-width: 0;
`;

const VerticalDivider = styled.div<{ $margin: number }>`
    align-self: stretch;
    width: 0.5px;
    margin: 0 ${({ $margin }) => $margin}px;
    background-color: ${(props) => props.theme.colors.border};
`;

export default function SearchControl() {
    const { t } = useTranslation('lineage');
    const { searchQuery, setSearchQuery, setSearchedEntity } = useContext(LineageVisualizationContext);
    const [isFocused, setIsFocused] = useState(false);
    const inputRef = useRef<InputRef>(null);

    useCaptureKeyboardSearch(inputRef, setIsFocused);

    const matchedNodes = useComputeMatchedNodes();
    const searchIndex = useAssignSearchedEntity(matchedNodes);

    const prev = useCallback(
        () => setSearchedEntity(matchedNodes[getWrappedIndex(searchIndex, -1, matchedNodes.length)]),
        [matchedNodes, searchIndex, setSearchedEntity],
    );
    const next = useCallback(
        () => setSearchedEntity(matchedNodes[getWrappedIndex(searchIndex, 1, matchedNodes.length)]),
        [matchedNodes, searchIndex, setSearchedEntity],
    );

    const isOpen = isFocused || !!searchQuery;
    const close = useCallback(() => {
        setSearchQuery('');
        setIsFocused(false);
        setTimeout(() => inputRef.current?.blur(), 0);
    }, [setSearchQuery]);
    useCaptureEscape(isOpen, close);

    const openSearch = useCallback(() => {
        setIsFocused(true);
        setTimeout(() => inputRef.current?.focus(), 0);
    }, []);

    return (
        <StyledPanel position="top-left">
            <SearchRow
                $width={isOpen ? OPEN_WIDTH : COLLAPSED_SIZE}
                onFocusCapture={() => setIsFocused(true)}
                onBlurCapture={(e) => {
                    if (!e.currentTarget.contains(e.relatedTarget as Node | null)) {
                        setIsFocused(false);
                    }
                }}
            >
                {isOpen ? (
                    <StyledSearchBar
                        ref={inputRef}
                        value={searchQuery}
                        onChange={(value) => setSearchQuery(value)}
                        placeholder={t('controls.search.placeholder')}
                        height={SEARCH_BAR_HEIGHT}
                        allowClear={false}
                        onKeyDown={(e) => {
                            if (e.key === 'Enter') {
                                if (e.shiftKey) prev();
                                else next();
                            }
                        }}
                    />
                ) : (
                    <CollapsedSearch onClick={openSearch}>
                        <LineageControlIcon icon={MagnifyingGlass} color="icon" />
                    </CollapsedSearch>
                )}
                {isOpen && searchQuery && (
                    <>
                        {/* eslint-disable i18next/no-literal-string -- (untranslated-text) numeric match-position separator (e.g. "3 / 10"), not translatable */}
                        <span>
                            {matchedNodes.length ? searchIndex + 1 : 0} / {matchedNodes.length}
                        </span>
                        {/* eslint-enable i18next/no-literal-string */}
                        <VerticalDivider $margin={8} />
                        <Button
                            icon={{ icon: CaretUp, weight: LINEAGE_CONTROL_ICON_WEIGHT }}
                            variant="outline"
                            size="sm"
                            onClick={prev}
                        />
                        <Button
                            icon={{ icon: CaretDown, weight: LINEAGE_CONTROL_ICON_WEIGHT }}
                            variant="outline"
                            size="sm"
                            onClick={next}
                        />
                        <Button
                            icon={{ icon: X, weight: LINEAGE_CONTROL_ICON_WEIGHT }}
                            variant="outline"
                            size="sm"
                            onClick={close}
                        />
                    </>
                )}
            </SearchRow>
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
            return [0, matchedNodes.length ? matchedNodes[0] : null];
        }
        const index = matchedNodes.indexOf(searchedEntity);
        if (index === -1) {
            return [0, matchedNodes[0]];
        }
        return [index, searchedEntity];
    }, [searchedEntity, matchedNodes]);

    useDebounce(() => setSearchedEntity(newSearchedEntity), 300, [searchQuery]);

    return searchIndex;
}

function useCaptureKeyboardSearch(inputRef: React.RefObject<InputRef>, setIsFocused: (value: boolean) => void) {
    const { isFocused } = useContext(LineageVisualizationContext);

    const handleKeyPress = useCallback(
        (e: KeyboardEvent) => {
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
