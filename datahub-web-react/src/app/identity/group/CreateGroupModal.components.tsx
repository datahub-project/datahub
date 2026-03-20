import styled from 'styled-components/macro';

import { Button } from '@src/alchemy-components';

export const FormSection = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;
    margin-bottom: 16px;
`;

export const AdvancedContent = styled.div`
    margin-top: 12px;
`;

export const AdvancedButton = styled(Button)`
    padding-left: 0;
    padding-right: 0;
    color: ${(props) => props.theme.colors.textSecondary};
`;
