import React, { useContext } from 'react';
import styled from 'styled-components';
import KeyboardDoubleArrowDownIcon from '@mui/icons-material/KeyboardDoubleArrowDown';
import { LINEAGE_FILTER_ID_PREFIX, LineageFilter, LineageNodesContext, LINEAGE_FILTER_PAGINATION } from '../common';
import { ANTD_GRAY, SEARCH_COLORS } from '../../entityV2/shared/constants';

const Wrapper = styled.div`
    align-items: center;
    display: flex;
    flex-shrink: 0;
    justify-content: center;
`;

const Button = styled.div`
    all: unset;
    align-items: center;
    border-radius: 20px;
    color: ${ANTD_GRAY[9]};
    cursor: pointer;
    display: flex;
    font-size: 10px;
    margin-left: 4px;
    padding: 2px 4px;
    width: fit-content;

    :hover {
        background-color: ${SEARCH_COLORS.TITLE_PURPLE}10;
        color: ${SEARCH_COLORS.TITLE_PURPLE};
    }
`;

const Text = styled.span`
    margin-right: 2px;
`;

interface Props {
    id: string;
    data: LineageFilter;
}

export function ShowMoreButton({ id, data }: Props) {
    const { direction, contents } = data;
    const urn = id.slice(LINEAGE_FILTER_ID_PREFIX.length + 2); // +2 for the direction section

    const { nodes, setDisplayVersion } = useContext(LineageNodesContext);

    function showMore() {
        const filters = nodes.get(urn)?.filters?.[direction];
        if (filters?.limit && filters.limit < contents.length) {
            // To zoom in on newly added nodes on click, uncomment below and pass into setDisplayVersion
            // Should match useProcessData.applyFilters logic
            // const newNodes = [
            //     id,
            //     ...contents.slice(contents.length - filters.limit - increment, contents.length - filters.limit),
            // ];
            filters.limit += LINEAGE_FILTER_PAGINATION;
            setDisplayVersion(([v, n]) => [v + 1, n]);
        }
    }

    return (
        <Wrapper className="show-more">
            <Button onClick={showMore}>
                <Text>Show More</Text>
                <KeyboardDoubleArrowDownIcon fontSize="inherit" />
            </Button>
        </Wrapper>
    );
}
