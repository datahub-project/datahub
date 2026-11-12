/**
 * Shared styled components for the tool picker and the forms around it.
 */
import styled from 'styled-components';

export const FormField = styled.div`
    display: flex;
    flex-direction: column;
    align-items: stretch;
    gap: 4px;
    margin-bottom: 16px;
`;

// Matches the alchemy <Input> / <TextArea> label so manual section headers
// (Tools, Scope) read identically to the built-in labels for Name, Description, etc.
export const Label = styled.label`
    font-weight: 700;
    font-size: 12px;
    color: ${(props) => props.theme.colors.text};
`;

export const FieldHeader = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
`;

export const HelpText = styled.span`
    font-size: 12px;
    color: ${(props) => props.theme.colors.textSecondary};
`;

export const ModalScrollBody = styled.div`
    max-height: 60vh;
    overflow-y: auto;
    /* Symmetric horizontal padding so input focus rings aren't clipped on the
       left edge (overflow-y: auto implicitly clips horizontal overflow). */
    padding: 2px 4px;
`;
