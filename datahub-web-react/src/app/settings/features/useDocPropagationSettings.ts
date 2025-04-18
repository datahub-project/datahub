import { useEffect, useState } from 'react';

import { message } from 'antd';

import {
    useGetDocPropagationSettingsQuery,
    useUpdateDocPropagationSettingsMutation,
} from '../../../graphql/app.generated';

// Hook to get the document propagation settings & manage state
export const useGetDocPropagationSettings = () => {
    const { data, refetch } = useGetDocPropagationSettingsQuery();
    const [isColPropagateChecked, setIsColPropagateChecked] = useState<boolean>(false);

    useEffect(() => {
        const docPropSetting = data?.docPropagationSettings?.docColumnPropagation;
        if (docPropSetting !== undefined) setIsColPropagateChecked(!!docPropSetting);
    }, [data]);

    return {
        isColPropagateChecked,
        setIsColPropagateChecked,
        refetch,
    };
};

// Hook to update the document propagation settings
export const useUpdateDocPropagationSettings = () => {
    const [updateDocPropagationSettings] = useUpdateDocPropagationSettingsMutation();
    const { refetch } = useGetDocPropagationSettingsQuery();

    const updateDocPropagation = async (checked: boolean) => {
        try {
            await updateDocPropagationSettings({
                variables: {
                    input: {
                        docColumnPropagation: checked,
                    },
                },
            });
            refetch();
            message.success('Successfully updated documentation propagation settings');
        } catch (e) {
            message.error('Failed to update documentation propagation settings');
            refetch();
        }
    };

    return { updateDocPropagation };
};
