import { EmptyState } from '@components';
import { BookmarksSimple } from '@phosphor-icons/react/dist/csr/BookmarksSimple';
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

// Fills the remaining vertical space inside the parent flex column so the
// EmptyState lands in the optical center of the page rather than hugging the
// top-left of whatever container it sits in.
const CenteredWrapper = styled.div`
    flex: 1;
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 24px;
    min-height: 240px;
`;

interface Props {
    title?: string;
    description?: string;
    onAddTerm: () => void;
    onAddtermGroup: () => void;
}

function EmptyGlossarySection(props: Props) {
    const { t } = useTranslation('governance.glossary');
    const { title, description, onAddTerm, onAddtermGroup } = props;

    // Alchemy EmptyState requires a non-empty title. Some callers (e.g. the
    // glossary-node ChildrenTab) only have a single line of copy to show — for
    // those we promote `description` into the title slot so we never render an
    // empty heading.
    const resolvedTitle = title ?? description ?? '';
    const resolvedDescription = title ? description : undefined;

    return (
        <CenteredWrapper>
            <EmptyState
                icon={BookmarksSimple}
                title={resolvedTitle}
                description={resolvedDescription}
                action={{
                    label: t('empty.addTerm'),
                    icon: { icon: Plus },
                    onClick: onAddTerm,
                    dataTestId: 'add-term-button',
                }}
                secondaryAction={{
                    label: t('empty.addTermGroup'),
                    icon: { icon: Plus },
                    variant: 'secondary',
                    onClick: onAddtermGroup,
                    dataTestId: 'add-term-group-button',
                }}
            />
        </CenteredWrapper>
    );
}

export default EmptyGlossarySection;
