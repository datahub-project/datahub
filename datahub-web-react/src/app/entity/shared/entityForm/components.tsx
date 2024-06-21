import styled from 'styled-components';

export const Flex = styled.div`
    display: flex;
    flex-direction: column;
    flex: 1;
    align-items: center;
    text-align: center;
    padding: 4rem;

    > h4 {
        font-weight: 600;
        font-size: 20px;
        margin-bottom: 0.25rem;

        > span {
            margin-right: 0.5rem;
        }
    }

    > p {
        font-size: 16px;
        max-width: 540px;
    }
`;
