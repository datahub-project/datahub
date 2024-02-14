import { Popover, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { CorpGroup, CorpUser } from '../../../../../types.generated';
import { ExpandedActor } from './ExpandedActor';

const PopoverActors = styled.div`
    max-width: 600px;
`;

const ActorsContainer = styled.div`
    display: flex;
    justify-content: right;
    flex-wrap: wrap;
    align-items: center;
`;

const RemainderText = styled(Typography.Text)`
    display: flex;
    justify-content: right;
    margin-right: 8px;
`;

type Props = {
    actors: Array<CorpUser | CorpGroup>;
    max: number;
    onClose?: (actor: CorpUser | CorpGroup) => void;
    containerStyle?: any;
};

const DEFAULT_MAX = 10;

export const ExpandedActorGroup = ({ actors, max = DEFAULT_MAX, onClose, containerStyle }: Props) => {
    const finalActors = actors.length > max ? actors.slice(0, max) : actors;
    const remainder = actors.length > max ? actors.length - max : undefined;

    return (
        <Popover
            placement="left"
            content={
                <PopoverActors>
                    {actors.map((actor) => (
                        <ExpandedActor key={actor.urn} actor={actor} onClose={() => onClose?.(actor)} />
                    ))}
                </PopoverActors>
            }
        >
            <ActorsContainer style={containerStyle}>
                {finalActors.map((actor) => (
                    <ExpandedActor key={actor.urn} actor={actor} onClose={() => onClose?.(actor)} />
                ))}
            </ActorsContainer>
            {remainder && <RemainderText type="secondary">+ {remainder} more</RemainderText>}
        </Popover>
    );
};
