import React from 'react';
import { CorpGroup, CorpUser } from '../../../../../types.generated';
import { ExpandedActor } from './ExpandedActor';

type Props = {
    actors: Array<CorpUser | CorpGroup>;
    onClose?: (actor: CorpUser | CorpGroup) => void;
};

export const ExpandedActorGroup = ({ actors, onClose }: Props) => {
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
