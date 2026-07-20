import styled from 'styled-components/macro';

// Row chrome shared between NodeItem and TermItem so the two leaf types in the glossary tree
// stay visually identical. Mirrors `DomainNode` and `DocumentTreeItem` (the sibling row
// components in the domains and documents sidebars): 38px row, 6px radius, brand-tinted
// selected state, neutral hover background + focus shadow, level-based left indent
// (8 + depth * 16). If any of these tokens change, edit them here once.

export const TreeRowContainer = styled.div<{ $level: number; $isSelected: boolean }>`
    position: relative;
    display: flex;
    align-items: center;
    padding: 4px 8px 4px ${(props) => 8 + props.$level * 16}px;
    min-height: 38px;
    height: 38px;
    cursor: pointer;
    border-radius: 6px;
    transition: background-color 0.15s ease;
    margin: 0 2px 2px 2px;

    ${(props) =>
        props.$isSelected &&
        `
        background: ${props.theme.colors.bgSelectedSubtle};
        box-shadow: ${props.theme.colors.shadowFocusBrand};
    `}

    ${(props) =>
        !props.$isSelected &&
        `
        &:hover {
            background: ${props.theme.colors.bgHover};
            box-shadow: ${props.theme.colors.shadowFocus};
        }
    `}
`;

export const TreeRowLeftContent = styled.div`
    display: flex;
    align-items: center;
    flex: 1;
    min-width: 0;
    overflow: hidden;
`;

/** 24x20 icon slot — matches `DomainNode` / `DocumentTreeItem` so vertical alignment matches
 * across all three sidebars. */
export const TreeRowIconSlot = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    width: 24px;
    height: 20px;
    margin-right: 8px;
    flex-shrink: 0;
`;

export const TreeRowTitle = styled.span<{ $isSelected: boolean }>`
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    font-size: 14px;
    line-height: 20px;
    color: ${(props) => props.theme.colors.textSecondary};

    ${(props) =>
        props.$isSelected &&
        `
        background: ${props.theme.colors.brandGradientSelected};
        background-clip: text;
        -webkit-text-fill-color: transparent;
        font-weight: 600;
    `}
`;
