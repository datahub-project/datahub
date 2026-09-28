import { Icon, Text } from '@components';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import React from 'react';
import styled from 'styled-components';

const Header = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

const IconSlot = styled.div<{ $hasCrumbs: boolean }>`
    display: flex;
    align-items: center;
    justify-content: center;
    flex-shrink: 0;
    /* Mulish's ascent (~1.005em) is far taller than its cap height (~0.72em), so the text's ink
       sits below the centre of its line boxes. In the two-line layout that makes a geometrically
       centred icon read as top-aligned; shift it onto the optical centre instead. */
    ${({ $hasCrumbs }) => $hasCrumbs && 'margin-top: 2px;'}
`;

const TitleBlock = styled.div`
    display: flex;
    flex-direction: column;
    flex: 1;
    min-width: 0;
`;

const Title = styled.div`
    color: ${(props) => props.theme.colors.text};
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

const Crumbs = styled.div`
    display: flex;
    align-items: center;
    gap: 4px;
    color: ${(props) => props.theme.colors.textTertiary};
    overflow: hidden;
`;

const Crumb = styled.span`
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

const BadgeSlot = styled.div`
    display: flex;
    align-items: center;
    flex-shrink: 0;
`;

type Props = {
    title: string;
    icon?: React.ReactNode;
    typeName?: string;
    crumbs?: string[];
    badge?: React.ReactNode;
};

export default function HoverCardHeader({ title, icon, typeName, crumbs = [], badge }: Props) {
    const path = [typeName, ...crumbs].filter((crumb): crumb is string => !!crumb);

    return (
        <Header>
            {icon && <IconSlot $hasCrumbs={path.length > 0}>{icon}</IconSlot>}
            <TitleBlock>
                <Title>
                    {/* Tighter than the default `md` leading to close the gap to the crumbs below,
                        but still taller than the glyph box — `Title` clips overflow for its
                        ellipsis, so a shorter line box would cut the descenders. */}
                    <Text size="md" weight="semiBold" lineHeight="sm">
                        {title}
                    </Text>
                </Title>
                {path.length > 0 && (
                    <Crumbs>
                        {path.map((crumb, index) => (
                            <React.Fragment key={crumb}>
                                {index > 0 && <Icon icon={CaretRight} size="sm" color="inherit" />}
                                <Crumb>
                                    {/* `Crumb` clips for its ellipsis, so this needs a line box
                                        taller than the 14px glyph box or descenders get cut. */}
                                    <Text size="md" lineHeight="sm">
                                        {crumb}
                                    </Text>
                                </Crumb>
                            </React.Fragment>
                        ))}
                    </Crumbs>
                )}
            </TitleBlock>
            {badge && <BadgeSlot>{badge}</BadgeSlot>}
        </Header>
    );
}
