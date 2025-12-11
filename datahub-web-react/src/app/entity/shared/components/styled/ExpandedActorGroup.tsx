/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Popover, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { ExpandedActor } from '@app/entity/shared/components/styled/ExpandedActor';

import { CorpGroup, CorpUser } from '@types';

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
