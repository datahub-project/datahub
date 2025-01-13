import styled from 'styled-components';

export const AvatarListContainer = styled.div`
    position: relative;
    display: flex;
    align-items: flex-start;
`;

export const AvatarContainer = styled.div`
    margin-left: -10px;
    &:first-child {
        margin-left: 0;
    }
`;
