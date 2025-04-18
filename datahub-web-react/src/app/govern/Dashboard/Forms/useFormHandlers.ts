import { useContext } from 'react';
import ManageFormContext from './ManageFormContext';

// Custom Hook for Form Handlers
export const useFormHandlers = () => {
    const { setFormValues } = useContext(ManageFormContext);

    // Handler for input changes
    const handleInputChange = (event) => {
        const { id, value } = event.target;

        setFormValues((prev) => ({
            ...prev,
            [id]: value,
        }));
    };

    // Handler for select changes
    const handleSelectChange = (fieldName, value) => {
        setFormValues((prev) => ({
            ...prev,
            [fieldName]: value,
        }));
    };

    const handleOwnersCheckBox = (event) => {
        setFormValues((prev) => ({
            ...prev,
            actors: {
                ...prev.actors,
                owners: event.target.checked,
            },
        }));
    };

    return { handleInputChange, handleSelectChange, handleOwnersCheckBox };
};
