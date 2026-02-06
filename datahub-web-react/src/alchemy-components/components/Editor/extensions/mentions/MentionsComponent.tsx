import { LoadingOutlined } from '@ant-design/icons';
import { FloatingWrapper, useRemirrorContext } from '@remirror/react';
import { Empty, Spin } from 'antd';
import React, { useEffect, useMemo, useState } from 'react';
import { createPortal } from 'react-dom';
import { useDebounce } from 'react-use';
import { Positioner, selectionPositioner } from 'remirror/extensions';
import styled from 'styled-components';

import { MentionsDropdown } from '@components/components/Editor/extensions/mentions/MentionsDropdown';
import { useDataHubMentions } from '@components/components/Editor/extensions/mentions/useDataHubMentions';
import { calculateMentionsPlacement } from '@components/components/Editor/extensions/mentions/utils';
import { colors } from '@components/theme';

import { useUserContext } from '@src/app/context/useUserContext';
import { useGetAutoCompleteMultipleResultsLazyQuery } from '@src/graphql/search.generated';

/**
 * Single source of truth for dropdown visual styling.
 * Used by both FloatingWrapper (document editor) and portal (chat sidebar).
 */
const DropdownWrapper = styled.div`
    background: white;
    border-radius: 12px;
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.1);
    overflow: hidden;

    .ant-spin-container {
        max-height: 300px;
        overflow-y: auto;
    }
`;

/** Fixed-position wrapper for portal rendering. Only handles positioning, not visuals. */
const PortalPositioner = styled.div<{ $bottom: string; $left: number }>`
    position: fixed;
    bottom: ${(props) => props.$bottom};
    left: ${(props) => props.$left}px;
    width: min(400px, calc(100vw - 32px));
    max-width: calc(100vw - ${(props) => props.$left}px - 16px);
    z-index: 10000;
`;

const StyledEmpty = styled(Empty)`
    margin: 16px;
    color: ${colors.gray[400]};
`;

interface MentionsComponentProps {
    /** When true, renders the dropdown outside the editor to avoid overflow clipping */
    renderOutsideEditor?: boolean;
}

export const MentionsComponent = ({ renderOutsideEditor = false }: MentionsComponentProps) => {
    const userContext = useUserContext();
    const { view } = useRemirrorContext();
    const [getAutoComplete, { data: autocompleteData, loading }] = useGetAutoCompleteMultipleResultsLazyQuery();
    const { active, range, filter: query } = useDataHubMentions({});
    const [suggestions, setSuggestions] = useState<any[]>([]);
    const viewUrn = userContext.localState?.selectedViewUrn;

    // Calculate dropdown position relative to the editor container (not the @ symbol)
    // This positions the dropdown above the entire input, like John's original implementation
    const portalPosition = useMemo(() => {
        if (!renderOutsideEditor || !active || !view?.dom) return null;
        const rect = view.dom.getBoundingClientRect();
        // Position 8px above the top of the editor
        const top = rect.top - 8;
        return {
            bottom: `calc(100vh - ${top}px)`,
            left: rect.left,
        };
    }, [renderOutsideEditor, active, view]);

    // For FloatingWrapper: calculate placement based on cursor position in viewport
    // If cursor is near bottom of viewport, show dropdown above to avoid clipping
    const floatingPlacement = useMemo(() => {
        if (renderOutsideEditor || !active || !range || !view?.dom) return 'bottom-start';
        try {
            const cursorCoords = view.coordsAtPos(range.from);
            return calculateMentionsPlacement(cursorCoords.bottom, window.innerHeight);
        } catch {
            return 'bottom-start';
        }
    }, [renderOutsideEditor, active, range, view]);

    useEffect(() => {
        if (query) {
            getAutoComplete({ variables: { input: { query, viewUrn } } });
        }
    }, [getAutoComplete, query, viewUrn]);
    useDebounce(() => setSuggestions(autocompleteData?.autoCompleteForMultiple?.suggestions || []), 250, [
        autocompleteData,
    ]);

    if (!active) return null;

    const mentionsPositioner = selectionPositioner.clone(() => ({
        getActive: ({ view: v }) => {
            try {
                if (!range) return Positioner.EMPTY;
                return [{ from: v.coordsAtPos(range.from), to: v.coordsAtPos(range.to) }];
            } catch {
                return Positioner.EMPTY;
            }
        },
    }));

    const dropdownContent = (
        <Spin spinning={loading} delay={100} indicator={<LoadingOutlined />}>
            {suggestions?.length > 0 ? (
                <MentionsDropdown suggestions={suggestions} />
            ) : (
                <StyledEmpty image={Empty.PRESENTED_IMAGE_SIMPLE} description="No results found" />
            )}
        </Spin>
    );

    // When renderOutsideEditor is true, use portal to document.body with fixed positioning
    // This escapes all overflow containers and positions above the editor
    if (renderOutsideEditor && portalPosition) {
        return createPortal(
            <PortalPositioner
                className="mentions-dropdown-portal"
                $bottom={portalPosition.bottom}
                $left={portalPosition.left}
            >
                <DropdownWrapper>{dropdownContent}</DropdownWrapper>
            </PortalPositioner>,
            document.body,
        );
    }

    // Default: use FloatingWrapper for in-editor positioning (follows @ symbol)
    // Placement is dynamic based on cursor position - flips to top when near bottom of viewport
    return (
        <FloatingWrapper positioner={mentionsPositioner} enabled={active} placement={floatingPlacement}>
            <DropdownWrapper>{dropdownContent}</DropdownWrapper>
        </FloatingWrapper>
    );
};
