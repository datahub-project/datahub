import React from 'react';
import { CorpGroup, CorpUser } from '../../../../../types.generated';
import { ExpandedActor } from './ExpandedActor';

type Props = {
    actors: Array<CorpUser | CorpGroup>;
    onClose?: (actor: CorpUser | CorpGroup) => void;
};

export const ExpandedActorGroup = ({ actors, onClose }: Props) => {
    return (
        <>
            {actors.map((actor) => (
                <ExpandedActor key={actor.urn} actor={actor} onClose={() => onClose?.(actor)} />
            ))}
        </>
    );
};
