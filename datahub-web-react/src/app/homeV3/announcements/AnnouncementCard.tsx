import { Card, Icon, Text, colors } from '@components';
import React from 'react';
import styled from 'styled-components';

import { StyledIcon } from '@app/homeV3/styledComponents';
import { Editor } from '@src/alchemy-components/components/Editor/Editor';

import { Post } from '@types';

const StyledEditor = styled(Editor)`
    border: none;

    &&& {
        .remirror-editor {
            padding: 0;
            color: ${colors.violet[500]};

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

const cardStyles = {
    backgroundColor: colors.violet[0],
    padding: '8px',
    border: 'none',
};

interface Props {
    announcement: Post;
}

export const AnnouncementCard = ({ announcement }: Props) => {
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
            button={<StyledIcon icon="X" source="phosphor" color="violet" size="xl" onClick={() => console.log('')} />}
            width="100%"
            style={cardStyles}
            isCardClickable={false}
        />
    );
};
