import React from 'react';
import styled from 'styled-components';

import { Text } from '@src/alchemy-components';
import { COLOR_SCHEME_TO_PARAMS, DEFAULT_COLOR_SCHEME } from '@src/alchemy-components/components/BarChart/constants';
import { Datum } from '@src/alchemy-components/components/BarChart/types';
import { formatNumberWithoutAbbreviation } from '@src/app/shared/formatNumber';
import { pluralize } from '@src/app/shared/textUtil';

interface RowCountByValuePopoverProps {
    datum: Datum;
    labelFormatter: (datum: Datum) => React.ReactNode;
}

const Container = styled.div`
    display: flex;
    flex-direction: row;
    gap: 8px;
    align-items: center;
`;

const Square = styled.div<{ $startColor: string; $endColor: string }>`
    display: flex;
    height: 12px;
    width: 12px;
    border-radius: 4px;
    background: ${(props) => `linear-gradient(270deg, ${props.$startColor} 0%, ${props.$endColor} 100%)`};
`;

const TruncatedText = styled.div`
    max-width: 100px;
    text-wrap: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
`;

const LabelContainer = styled.div`
    display: flex;
    flex-direction: row;
`;

export default function RowCountByValuePopover({ datum, labelFormatter }: RowCountByValuePopoverProps) {
    const colorScheme = datum.colorScheme ?? DEFAULT_COLOR_SCHEME;
    const colorSchemeParams = COLOR_SCHEME_TO_PARAMS[colorScheme];

    return (
        <Container>
            <Square $startColor={colorSchemeParams.mainColor} $endColor={colorSchemeParams.alternativeColor} />
            <Text color="gray" size="sm">
                <LabelContainer>
                    <TruncatedText>{labelFormatter(datum)}</TruncatedText>:
                </LabelContainer>
            </Text>
            <Text color="gray" size="sm" weight="semiBold">
                {formatNumberWithoutAbbreviation(datum.x)} {pluralize(datum.x, 'Row')}
            </Text>
        </Container>
    );
}
