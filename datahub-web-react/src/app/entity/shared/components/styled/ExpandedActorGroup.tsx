import { Popover, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { CorpGroup, CorpUser } from '../../../../../types.generated';
import { ExpandedActor } from './ExpandedActor';

const PopoverActors = styled.div`
    max-width: 260px;
`;

type Props = {
    actors: Array<CorpUser | CorpGroup>;
    max?: number | null;
    onClose?: (actor: CorpUser | CorpGroup) => void;
};

const DEFAULT_MAX = 10;

export const ExpandedActorGroup = ({ actors, max, onClose }: Props) => {
    const finalMax = max || DEFAULT_MAX;
    const finalActors = actors.length > finalMax ? actors.slice(0, finalMax) : actors;
    const remainder = actors.length > finalMax ? actors.length - finalMax : undefined;

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
            <div style={{ display: 'flex', justifyContent: 'right', flexWrap: 'wrap', alignItems: 'center' }}>
                {finalActors.map((actor) => (
                    <ExpandedActor key={actor.urn} actor={actor} onClose={() => onClose?.(actor)} />
                ))}
                {remainder && (
                    <Typography.Text style={{ marginBottom: 8 }} type="secondary">
                        + {remainder} more
                    </Typography.Text>
                )}
            </div>
        </Popover>
    );
};
