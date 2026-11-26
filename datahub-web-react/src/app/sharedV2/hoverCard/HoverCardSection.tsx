import { Text } from '@components';
import React from 'react';
import styled from 'styled-components';

// 4px between a section's label and its content, against 8px between sections — the label stays
// visibly bound to what it labels while the sections still read as separate blocks.
const Section = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;
    width: 100%;
`;

const SectionTitle = styled.div`
    color: ${(props) => props.theme.colors.text};
`;

const SectionContent = styled.div`
    color: ${(props) => props.theme.colors.textSecondary};
`;

type Props = {
    title?: string;
    children?: React.ReactNode;
    className?: string;
};

export default function HoverCardSection({ title, children, className }: Props) {
    // A title on its own is a real section — an ownership role often has a name and no
    // description, and dropping the whole section hid the role.
    if (!title && !children) return null;

    return (
        <Section className={className}>
            {title && (
                <SectionTitle>
                    {/* Tight leading so the space below the label is the 4px `Section` gap rather
                        than 4px plus the font's phantom leading. Safe to trim here — unlike the
                        header title, `SectionTitle` doesn't clip overflow for an ellipsis. */}
                    <Text size="md" weight="bold" lineHeight="xs">
                        {title}
                    </Text>
                </SectionTitle>
            )}
            {children ? <SectionContent>{children}</SectionContent> : null}
        </Section>
    );
}
