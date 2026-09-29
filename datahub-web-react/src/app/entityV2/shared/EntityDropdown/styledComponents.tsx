import styled from 'styled-components';

export const ENTITY_HEADER_ACTION_ICON_SIZE = 16;
export const ENTITY_HEADER_ACTION_ICON_WEIGHT = 'regular' as const;

// `type="button"` by default so these icon actions never submit an enclosing form.
export const ActionMenuItem = styled.button.attrs<{ $fontSize?: number }>({ type: 'button' })<{
    $fontSize?: number;
}>`
    flex-shrink: 0;
    width: ${(props) => (props.$fontSize ? `${props.$fontSize}px` : '28px')};
    height: ${(props) => (props.$fontSize ? `${props.$fontSize}px` : '28px')};
    padding: 0;
    display: flex;
    align-items: center;
    justify-content: center;
    border: none;
    border-radius: 50%;
    background: none;
    color: ${(props) => props.theme.colors.icon};
    cursor: pointer;
    /* Buttons don't inherit the page font by default; without this, Phosphor glyphs sized in
       em units fall back to the browser's form-control size and render noticeably small. */
    font: inherit;

    &:hover {
        background-color: ${(props) => props.theme.colors.bgSurface};
        color: ${(props) => props.theme.colors.textHover};
    }

    &:disabled {
        color: ${(props) => props.theme.colors.textDisabled};
        cursor: not-allowed;
        background: none;
    }
`;
