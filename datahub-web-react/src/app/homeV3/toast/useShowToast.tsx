import { Text, toast } from '@components';
import React, { useCallback } from 'react';
import styled from 'styled-components';

const Content = styled.div`
    display: flex;
    flex-direction: column;
    gap: 2px;
`;

export default function useShowToast() {
    const showToast = useCallback((title: string, description?: string, dataTestId?: string) => {
        toast.info(
            <Content>
                <Text weight="semiBold" lineHeight="sm" data-testid={dataTestId}>
                    {title}
                </Text>
                {description && <Text lineHeight="sm">{description}</Text>}
            </Content>,
            { duration: 0, placement: 'bottomRight', key: dataTestId },
        );
    }, []);

    return { showToast };
}
