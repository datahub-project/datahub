import React from 'react';
import styled from 'styled-components';
import { Popover } from 'antd';
import { StringMapEntry } from '../../../../types.generated';
import PropagationEntityLink from './PropagationEntityLink';
import { usePropagationDetails } from './utils';
import { PropagateThunderbolt, PropagateThunderboltFilled } from './PropagationIcon';

const PopoverWrapper = styled.div`
    display: flex;
    flex-direction: column;
`;

const PopoverTitle = styled.div`
    font-weight: bold;
    font-size: 14px;
    padding: 6px 0px;
    color: #eeecfa;
`;

const PopoverDescription = styled.div`
    max-width: 340px;
    font-size: 14px;
    color: #eeecfa;
    display: inline;
    padding: 0px 0px 8px 0px;
`;

const PopoverAttributes = styled.div`
    display: flex;
`;

const PopoverAttribute = styled.div`
    margin-right: 12px;
    margin-bottom: 4px;
`;

const PopoverAttributeTitle = styled.div`
    font-size: 14px;
    color: #eeecfa;
    font-weight: bold;
    margin: 8px 0px;
    overflow: hidden;
    text-overflow: ellipsis;
`;

const PopoverDocumentation = styled.a`
    margin-top: 12px;
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
                    This description was automatically propagated from an upstream column.{' '}
                    <PopoverDocumentation
                        target="_blank"
                        rel="noreferrer"
                        href="https://datahubproject.io/docs/automations/docs-propagation?utm_source=datahub_core&utm_medium=docs&utm_campaign=propagation_details"
                    >
                        Learn more
                    </PopoverDocumentation>
                </PopoverDescription>
                <PopoverAttributes>
                    {originEntity && originEntity.urn !== viaEntity?.urn && (
                        <PopoverAttribute>
                            <PopoverAttributeTitle>Origin</PopoverAttributeTitle>
                            <PropagationEntityLink entity={originEntity} />
                        </PopoverAttribute>
                    )}
                    {viaEntity && (
                        <PopoverAttribute>
                            <PopoverAttributeTitle>Via</PopoverAttributeTitle>
                            <PropagationEntityLink entity={viaEntity} />
                        </PopoverAttribute>
                    )}
                </PopoverAttributes>
            </PopoverWrapper>
        ) : undefined;

    return (
        <Popover
            overlayInnerStyle={{ backgroundColor: '#272D48' }}
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
