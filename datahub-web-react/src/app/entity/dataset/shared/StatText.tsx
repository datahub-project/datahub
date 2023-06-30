import styled from 'styled-components';

const StatText = styled.span<{ color: string }>`
    color: ${(props) => props.color};
`;

export default StatText;
