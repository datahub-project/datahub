import { Button } from 'antd';
import styled from 'styled-components';

export default styled(Button)`
    padding-top: 5px;
    padding-bottom: 5px;
    padding-right: 16px;
    padding-left: 16px;
    box-shadow: ${(props) => props.theme.colors.shadowXs};
    border: 1px solid ${(props) => props.theme.colors.border};

    font-size: 12px;
    font-weight: 500;
    line-height: 20px;
    vertical-align: top;
    border-radius: 5px;
`;
