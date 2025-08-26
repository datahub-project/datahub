import React, { useEffect, useMemo, useRef, useState } from 'react';
import styled from 'styled-components';

import { cleanSample } from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/Samples/utils';
import { Button, Text } from '@src/alchemy-components';

const MAX_LINES = 2;

const Container = styled.div`
    display: flex;
    flex-direction: row;
    justify-content: space-between;
    // FYI: additional padding to compensate that td (TableCell) has reduced right padding
    padding-right: 8px;
`;

const SampleText = styled(Text)`
    width: 350px;
    overflow: clip;
    text-overflow: ellipsis;
` as typeof Text;

const Value = styled.div`
    display: flex;
    flex-direction: column;
    justify-content: center;
    height: 40px;
`;

interface SampleValueCellProps {
    sample: string;
    onViewSample?: (sample: string) => void;
}

export default function SampleValueCell({ sample, onViewSample }: SampleValueCellProps) {
    const [isSampleTruncated, setIsSampleTruncated] = useState<boolean>(false);
    const containerRef = useRef<HTMLDivElement>(null);

    const cleanedSample = useMemo(() => cleanSample(sample, MAX_LINES), [sample]);

    const shouldShowViewButton = useMemo(() => {
        return isSampleTruncated || sample !== cleanedSample;
    }, [sample, cleanedSample, isSampleTruncated]);

    useEffect(() => {
        const checkTruncating = () => {
            const element = containerRef.current?.firstElementChild;
            setIsSampleTruncated(!!element && element.scrollWidth > element.clientWidth);
        };

        // setTimeout to make sure that elemen is ready for measuring
        const timeout = setTimeout(() => checkTruncating());

        return () => clearTimeout(timeout);
    }, [cleanedSample, setIsSampleTruncated]);

    return (
        <Container>
            <Value ref={containerRef}>
                <SampleText type="pre">{cleanedSample}</SampleText>
            </Value>
            {shouldShowViewButton && (
                <Button variant="text" onClick={() => onViewSample?.(sample)}>
                    View
                </Button>
            )}
        </Container>
    );
}
