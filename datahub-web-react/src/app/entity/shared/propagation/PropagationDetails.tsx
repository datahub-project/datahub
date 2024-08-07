import React from 'react';
import styled from 'styled-components';
import { ThunderboltOutlined } from '@ant-design/icons';
import { Popover } from 'antd';
import { StringMapEntry } from '../../../../types.generated';
import PropagationEntityLink from './PropagationEntityLink';
import { usePropagationDetails } from './utils';

const PopoverWrapper = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;
`;

const PropagateThunderbolt = styled(ThunderboltOutlined)`
    color: #8088a3;
    font-weight: bold;
    &:hover {
        color: #4b39bc;
    }
`;

const EntityWrapper = styled.span`
    display: inline-flex;
    align-items: center;
    gap: 2px;
`;

const PropagationPrefix = styled.span`
    color: black;
`;

interface Props {
    sourceDetail?: StringMapEntry[] | null;
}

export default function PropagationDetails({ sourceDetail }: Props) {
    const {
        isPropagated,
        origin: { entity: originEntity },
        via: { entity: viaEntity },
    } = usePropagationDetails(sourceDetail);

    if (!sourceDetail || !isPropagated) return null;

    const popoverContent =
        originEntity || viaEntity ? (
            <PopoverWrapper>
                Propagated description
                <br />
                {viaEntity && (
                    <EntityWrapper>
                        <PropagationPrefix>via:</PropagationPrefix>
                        <PropagationEntityLink entity={viaEntity} />
                    </EntityWrapper>
                )}
                {originEntity && originEntity.urn !== viaEntity?.urn && (
                    <EntityWrapper>
                        <PropagationPrefix>origin:</PropagationPrefix>
                        <PropagationEntityLink entity={originEntity} />
                    </EntityWrapper>
                )}
            </PopoverWrapper>
        ) : undefined;

    return (
        <Popover content={popoverContent}>
            <PropagateThunderbolt data-testid="docPropagationIndicator" />
        </Popover>
    );
}
