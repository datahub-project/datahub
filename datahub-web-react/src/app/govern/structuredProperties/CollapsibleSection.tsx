import { Icon, Text } from '@components';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import React, { useState } from 'react';

import { SectionContainer, SectionContent, SectionHeader } from '@app/govern/structuredProperties/styledComponents';

type Props = {
    title: string;
    children: React.ReactNode;
    defaultOpen?: boolean;
    dataTestId?: string;
};

/**
 * A form section the user can fold away. Alchemy's CollapsiblePanel is a bordered card with a
 * leading caret; these sections are separated by a rule with a trailing caret instead.
 */
const CollapsibleSection = ({ title, children, defaultOpen = false, dataTestId }: Props) => {
    const [isOpen, setIsOpen] = useState(defaultOpen);

    return (
        <SectionContainer data-testid={dataTestId}>
            <SectionHeader
                onClick={() => setIsOpen(!isOpen)}
                aria-expanded={isOpen}
                data-testid={dataTestId ? `${dataTestId}-header` : undefined}
            >
                <Text weight="bold" color="gray">
                    {title}
                </Text>
                <Icon icon={CaretRight} color="gray" size="lg" rotate={isOpen ? '90' : '0'} />
            </SectionHeader>
            <SectionContent $isOpen={isOpen}>{children}</SectionContent>
        </SectionContainer>
    );
};

export default CollapsibleSection;
