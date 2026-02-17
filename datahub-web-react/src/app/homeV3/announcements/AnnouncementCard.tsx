import { Card, Icon, Text } from '@components';
import React from 'react';
import styled, { useTheme } from 'styled-components';

import { StyledIcon } from '@app/homeV3/styledComponents';
import { Editor } from '@src/alchemy-components/components/Editor/Editor';

import { Post } from '@types';

const StyledEditor = styled(Editor)`
    border: none;

    &&& {
        .remirror-editor {
            padding: 0;
            color: ${(props) => props.theme.colors.iconBrand};

            p {
                margin-bottom: 0;
            }

            h1,
            h2,
            h3,
            h4,
            h5,
            h6 {
                color: inherit;
            }
        }
    }
`;

interface Props {
    announcement: Post;
    onDismiss: (urn: string) => void;
}

export const AnnouncementCard = ({ announcement, onDismiss }: Props) => {
    const theme = useTheme();

    const cardStyles = {
        backgroundColor: theme.colors.bgSurfaceBrand,
        padding: '8px',
        border: 'none',
    };
    return (
        <Card
            icon={<Icon icon="MegaphoneSimple" source="phosphor" color="violet" weight="fill" size="2xl" />}
            title={
                <Text color="violet" weight="semiBold" size="md" lineHeight="normal">
                    {announcement.content.title}
                </Text>
            }
            subTitle={
                announcement.content.description ? (
                    <StyledEditor content={announcement.content.description} readOnly />
                ) : undefined
            }
            button={
                <StyledIcon
                    icon="X"
                    source="phosphor"
                    color="violet"
                    size="xl"
                    onClick={() => onDismiss(announcement.urn)}
                />
            }
            width="100%"
            style={cardStyles}
            isCardClickable={false}
        />
    );
};
