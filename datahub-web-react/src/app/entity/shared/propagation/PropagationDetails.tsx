import React from 'react';
import styled from 'styled-components';
import { Popover } from 'antd';
import { StringMapEntry } from '../../../../types.generated';
import PropagationEntityLink from './PropagationEntityLink';
import { usePropagationDetails } from './utils';
import { ANTD_GRAY } from '../constants';
import { PropagateThunderbolt, PropagateThunderboltFilled } from './PropagationIcon';

const PopoverWrapper = styled.div`
    display: flex;
    flex-direction: column;
`;

const PopoverTitle = styled.div`
    font-weight: bold;
    font-size: 14px;
    padding: 6px 0px;
`;

const PopoverDescription = styled.div`
    max-width: 340px;
    font-size: 14px;
    color: ${ANTD_GRAY[7]};
    display: inline;
    padding: 0px 0px 4px 0px;
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
                <PopoverDescription>
                    This description was automatically propagated{' '}
                    {originEntity && originEntity.urn !== viaEntity?.urn && (
                        <>
                            from <PropagationEntityLink entity={originEntity} />
                        </>
                    )}
                    {viaEntity && (
                        <>
                            via <PropagationEntityLink entity={viaEntity} />
                        </>
                    )}
                </PopoverDescription>
            </PopoverWrapper>
        ) : undefined;

    return (
        <Popover
            showArrow={false}
            title={
                <PopoverTitle>
                    <PropagateThunderboltFilled />
                    Propagated Description
                </PopoverTitle>
            }
            content={popoverContent}
        >
            <PropagateThunderbolt data-testid="docPropagationIndicator" />
        </Popover>
    );
}
