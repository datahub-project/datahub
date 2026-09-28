import styled from 'styled-components';

import { Text, typography } from '@src/alchemy-components';

export const PageContainer = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    overflow: auto;
    margin: ${(props) => (props.$isShowNavBarRedesign ? '0' : '0 12px 12px 0')};
    padding: 16px 20px 20px 20px;
    border-radius: ${(props) =>
        props.$isShowNavBarRedesign ? props.theme.styles['border-radius-navbar-redesign'] : '8px'};
    display: flex;
    flex: 1;
    flex-direction: column;
    gap: 20px;
    background-color: ${(props) => props.theme.colors.bg};
    ${(props) => props.$isShowNavBarRedesign && 'max-height: calc(100vh - 88px);'};
    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
        box-shadow: ${props.theme.colors.shadowSm};
        margin: 5px;
    `}
`;

export const HeaderContainer = styled.div`
    display: flex;
    justify-content: space-between;
`;

export const TableContainer = styled.div`
    display: flex;
    overflow: auto;
    flex: 1;
`;

export const HeaderContent = styled.div`
    display: flex;
    flex-direction: column;
`;

export const ButtonContainer = styled.div`
    display: flex;
    align-self: center;
`;

export const DataContainer = styled.div`
    display: flex;
    flex-direction: column;
    width: calc(100% - 44px);
`;

const truncatedTextStyles = `
    display: block;
    max-width: 100%;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

export const PropName = styled(Text)`
    ${truncatedTextStyles}
    font-size: 14px;
    font-weight: 600;
    color: ${(props) => props.theme.colors.text};
    line-height: normal;

    :hover {
        cursor: pointer;
        text-decoration: underline;
    }
`;

export const PropDescription = styled(Text)`
    ${truncatedTextStyles}
    font-size: 14px;
    font-weight: 400;
    color: ${(props) => props.theme.colors.textSecondary};
    line-height: normal;
`;

export const NameColumn = styled.div`
    display: flex;
    gap: 12px;
    align-items: center;
`;

export const IconContainer = styled.div`
    height: 32px;
    width: 32px;
    display: flex;
    justify-content: center;
    align-items: center;
    gap: 12px;
    border-radius: 200px;
    background-color: ${(props) => props.theme.colors.bgSurfaceBrand};
`;

export const PillsContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

export const PillContainer = styled.div`
    display: flex;
`;

// A setting nested under the toggle that enables it.
export const SettingSubItem = styled.div`
    padding-top: 32px;
    padding-left: 24px;
`;

export const CheckboxContainer = styled.div``;

// Groups a toggle with the settings it reveals so they move together in the list.
export const CompoundedItemWrapper = styled.div``;

export const SectionContainer = styled.div`
    display: flex;
    flex-direction: column;
`;

// The separator runs the full width of the surrounding padding, so the header is pulled out of it.
export const SectionHeader = styled.button.attrs({ type: 'button' as const })`
    display: flex;
    align-items: center;
    justify-content: space-between;
    border: none;
    border-top: 1px solid ${(props) => props.theme.colors.border};
    background: none;
    cursor: pointer;
    text-align: left;
    font-family: ${typography.fonts.body};
    margin-left: -16px;
    width: calc(100% + 32px);
    padding: 16px;
    margin-top: 12px;
`;

// Kept mounted while collapsed so the fields inside hold on to focus and scroll position.
export const SectionContent = styled.div<{ $isOpen: boolean }>`
    display: ${(props) => (props.$isOpen ? 'flex' : 'none')};
    flex-direction: column;
    gap: 24px;
    padding: 12px 0;
`;

export const TogglesContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 32px;
`;

export const CreatedByContainer = styled.div`
    display: inline-flex;
    align-items: center;
    gap: 4px;
    padding: 2px 6px 2px 4px;
    border-radius: 20px;
    border: 1px solid ${(props) => props.theme.colors.border};

    :hover {
        cursor: pointer;
    }
`;

export const AllowedValuesSection = styled.div`
    display: flex;
    flex-direction: column;
`;

// A repeating list has no single input to hang a native label on, and the list starts empty, so
// the section owns its label. Mirrors the Alchemy Input label so it matches the fields around it.
export const AllowedValuesLabel = styled.div`
    color: ${(props) => props.theme.colors.text};
    display: flex;
    align-items: center;
    gap: 4px;
    margin-bottom: 4px;
    font-family: ${typography.fonts.body};
    font-size: ${typography.fontSizes.sm};
    font-weight: ${typography.fontWeights.bold};
`;

export const AllowedValuesRequired = styled.span`
    color: ${(props) => props.theme.colors.textError};
`;

// No nested overflow: the list grows with the form and the page scrolls.
export const ValuesContainer = styled.div`
    display: flex;
    flex-direction: column;
`;

export const AddButtonContainer = styled.div`
    display: flex;
    margin: 4px 4px 0 4px;

    button {
        width: 100%;
        justify-content: center;
    }
`;

export const FormContainer = styled.div`
    display: grid;
    gap: 24px;
`;

export const FieldError = styled.div`
    margin-top: 4px;
    font-size: ${typography.fontSizes.sm};
    color: ${(props) => props.theme.colors.textError};
`;

export const EmptyContainer = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    height: 100%;
    width: 100%;
    gap: 16px;

    svg {
        width: 160px;
        height: 160px;
    }
`;

export const ActionsContainer = styled.div`
    display: flex;
    justify-content: flex-end;
`;

export const CardIcons = styled.div`
    display: flex;
    justify-content: end;
    gap: 12px;

    div {
        border: 1px solid ${(props) => props.theme.colors.border};
        border-radius: 20px;
        width: 28px;
        height: 28px;
        padding: 4px;
        color: ${(props) => props.theme.colors.textTertiary};
        :hover {
            cursor: pointer;
        }
    }
`;
