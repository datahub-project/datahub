import styled from 'styled-components';

/**
 * Floating dropdown body used by entity-picker modals that overlay a browse tree (domain navigator,
 * glossary tree) over an autocomplete `Select`. Positioned absolute under the input; toggled by an
 * `isHidden` boolean that the parent flips on input focus/blur.
 *
 * Lived inside `AddTagsTermsModal` historically; lifted out so the file split into
 * `AddTagsModal` / `AddTermsModal` doesn't require five other modals (SetDomainModal v1+v2,
 * NodeParentSelect v1+v2, DomainParentSelect v1+v2, GlossarySelector, PolicyPrivilegeForm) to
 * change their imports every time `AddTagsTermsModal` is restructured.
 */
export const BrowserWrapper = styled.div<{
    isHidden: boolean;
    width?: string;
    maxHeight?: number;
    minWidth?: number;
    maxWidth?: number;
}>`
    background-color: ${(props) => props.theme.colors.bgSurface};
    border-radius: 5px;
    box-shadow: ${(props) => props.theme.colors.shadowMd};
    max-height: ${(props) => (props.maxHeight ? props.maxHeight : '380')}px;
    overflow: auto;
    position: absolute;
    transition: opacity 0.2s;
    width: ${(props) => (props.width ? props.width : '100%')};
    ${(props) => props.minWidth !== undefined && `min-width: ${props.minWidth}px;`}
    ${(props) => props.maxWidth !== undefined && `max-width: ${props.maxWidth}px;`}
    z-index: 1051;
    ${(props) =>
        props.isHidden &&
        `
        opacity: 0;
        height: 0;
    `}
`;
