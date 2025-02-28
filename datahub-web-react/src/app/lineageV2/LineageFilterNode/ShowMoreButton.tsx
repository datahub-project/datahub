import { applyOpacity } from '@app/sharedV2/colors/colorUtils';
import React, { useCallback, useContext, useMemo } from 'react';
import styled from 'styled-components';
import KeyboardDoubleArrowDownIcon from '@mui/icons-material/KeyboardDoubleArrowDown';
import { LineageFilter, LineageNodesContext, LINEAGE_FILTER_PAGINATION } from '../common';
import { ANTD_GRAY, REDESIGN_COLORS } from '../../entityV2/shared/constants';

const MAX_INCREASE = 100;
const LINE_HEIGHT = '1.5em';

const ExtraButtons = styled.div`
    display: none;
    position: absolute;
    top: calc(${LINE_HEIGHT} + 1px);

    flex-direction: column;
    align-items: end;
    background-color: ${REDESIGN_COLORS.WHITE};
`;

const Wrapper = styled.div`
    position: relative;
    flex-shrink: 0;

    display: flex;
    align-items: center;
    justify-content: right;
    min-width: 71px;

    :hover {
        ${ExtraButtons} {
            display: flex;
        }
    }
`;

const Button = styled.div`
    all: unset;
    align-items: center;
    border-radius: 20px;
    background-color: ${REDESIGN_COLORS.WHITE};
    color: ${ANTD_GRAY[9]};
    cursor: pointer;
    display: flex;
    font-size: 10px;
    padding: 2px 4px;
    width: fit-content;

    :hover {
        background-color: ${applyOpacity(REDESIGN_COLORS.TITLE_PURPLE, REDESIGN_COLORS.WHITE, 10)};
        color: ${REDESIGN_COLORS.TITLE_PURPLE};
    }
`;

const Text = styled.span`
    margin-right: 2px;
    line-height: ${LINE_HEIGHT};
`;

interface Props {
    data: LineageFilter;
    numMatches: number;
}

export function ShowMoreButton({ data, numMatches }: Props) {
    const { direction, contents, limit, parent } = data;
    const { nodes, setDisplayVersion } = useContext(LineageNodesContext);

    const maximum = numMatches || contents.length;

    const setPagination = useCallback(
        (value: number) => {
            const filters = nodes.get(parent)?.filters[direction];
            if (filters?.limit) {
                // To zoom in on newly added nodes on click, uncomment below and pass into setDisplayVersion
                // Should match useProcessData.applyFilters logic
                // const newNodes = [
                //     id,
                //     ...contents.slice(maximum - filters.limit - increment, maximum - filters.limit),
                // ];
                filters.limit = value;
                setDisplayVersion(([v, n]) => [v + 1, n]);
            }
        },
        [direction, parent, nodes, setDisplayVersion],
    );

    const buttons = useMemo(() => {
        const list: JSX.Element[] = [];
        if (limit < maximum) {
            list.push(
                <Button
                    key="show-more"
                    onClick={() => setPagination(Math.min(limit + LINEAGE_FILTER_PAGINATION, maximum))}
                >
                    <Text>{limit + LINEAGE_FILTER_PAGINATION >= maximum ? 'Show All' : 'Show More'}</Text>
                    <KeyboardDoubleArrowDownIcon fontSize="inherit" />
                </Button>,
            );
        }
        if (limit > LINEAGE_FILTER_PAGINATION) {
            list.push(
                <Button
                    key="show-less"
                    onClick={() => setPagination(Math.min(maximum, limit) - LINEAGE_FILTER_PAGINATION)}
                >
                    <Text>Show Less</Text>
                    <KeyboardDoubleArrowDownIcon fontSize="inherit" />
                </Button>,
            );
        }
        if (limit + LINEAGE_FILTER_PAGINATION < maximum && limit + MAX_INCREASE >= maximum) {
            list.push(
                <Button key="show-all" onClick={() => setPagination(maximum)}>
                    <Text>Show All</Text>
                    <KeyboardDoubleArrowDownIcon fontSize="inherit" />
                </Button>,
            );
        }
        if (limit + MAX_INCREASE < maximum) {
            list.push(
                <Button key="show-max" onClick={() => setPagination(limit + MAX_INCREASE)}>
                    <Text>Show +{MAX_INCREASE}</Text>
                    <KeyboardDoubleArrowDownIcon fontSize="inherit" />
                </Button>,
            );
        }
        return list;
    }, [limit, maximum, setPagination]);

    if (!buttons.length) return null;
    return (
        <Wrapper className="show-more">
            {buttons[0]}
            {buttons.length > 1 && <ExtraButtons>{buttons.slice(1)}</ExtraButtons>}
        </Wrapper>
    );
}
