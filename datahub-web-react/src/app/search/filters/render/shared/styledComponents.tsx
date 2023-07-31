import styled from 'styled-components';
import { MoreFilterOptionLabel } from '../../styledComponents';

export const SearchFilterWrapper = styled.div`
    padding: 0 25px 15px 25px;
    display: flex;
    align-items: center;
    justify-content: left;
`;

export const Title = styled.div`
    align-items: center;
    font-weight: bold;
    display: flex;
    justify-content: left;
    cursor: pointer;
`;

export const StyledMoreFilterOptionLabel = styled(MoreFilterOptionLabel)`
    justify-content: left;
`;
