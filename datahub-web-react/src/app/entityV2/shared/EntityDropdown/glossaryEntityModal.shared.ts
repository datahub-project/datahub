import { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

/**
 * Vertical-stack form-field wrapper used by both `CreateGlossaryEntityModal` (V1 + V2) and
 * `EditGlossaryEntityModal`. Extracted so a single visual tweak to the field rhythm doesn't have
 * to be applied to three files.
 */
export const Field = styled.div`
    display: flex;
    flex-direction: column;
    gap: 6px;
    margin-bottom: 20px;

    &:last-child {
        margin-bottom: 0;
    }
`;

/**
 * Rich-text editor container with the modal-specific border/radius/height. Shared between the
 * create and edit glossary modals so the editor never drifts visually between the two flows.
 */
export const EditorContainer = styled.div`
    height: 200px;
    overflow: auto;
    border: 1px solid ${(props) => props.theme.colors.border};
    border-radius: 12px;
`;

/** Maximum length we let users save for a glossary term / term-group name. Matches the
 * pre-alchemy antd Form.Item rule that previously enforced this. Exported as a constant so the
 * server-side limit (if any future validator is added) has a single number to track. */
export const GLOSSARY_NAME_MAX_LENGTH = 100;

/**
 * Hook-ified version of the trimmed name validation rule shared by `CreateGlossaryEntityModal`
 * (V1 + V2) and `EditGlossaryEntityModal`. Returns a translated error message when the name is
 * empty or exceeds the {@link GLOSSARY_NAME_MAX_LENGTH} limit, or `undefined` when the name is
 * valid. The caller decides when to surface the message (typically after the field is touched).
 */
export function useGlossaryNameValidation(stagedName: string, entityName: string | undefined): string | undefined {
    const { t } = useTranslation('entity.shared.entityDropdown');
    return useMemo(() => {
        const trimmed = stagedName.trim();
        if (!trimmed) return t('createGlossary.nameRequired', { entityName: entityName ?? '' });
        if (trimmed.length > GLOSSARY_NAME_MAX_LENGTH) return t('createGlossary.nameMaxLengthError');
        return undefined;
    }, [stagedName, entityName, t]);
}
