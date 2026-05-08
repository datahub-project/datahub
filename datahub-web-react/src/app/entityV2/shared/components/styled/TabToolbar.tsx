import styled from 'styled-components';

export default styled.div`
    display: flex;
    position: relative;
    z-index: 1;
    justify-content: space-between;
    height: 46px;
    border-bottom: 1px solid ${(props) => props.theme.colors.border};
    padding: 8px 16px;
    box-shadow: ${(props) => props.theme.colors.shadowXs};
    flex: 0 0 auto;
`;
