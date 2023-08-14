import React, { ReactNode, createContext, useContext, useMemo } from 'react';
import { MatchedField } from '../../../types.generated';

type FieldName = 'description';

type HighlightContextValue = {
    matchedFields: Array<MatchedField>;
} | null;

const HighlightContext = createContext<HighlightContextValue>(null);

type Props = {
    children: ReactNode;
    matchedFields: Array<MatchedField>;
};

export const HighlightProvider = ({ children, matchedFields = [] }: Props) => {
    const value = useMemo(
        () => ({
            matchedFields,
        }),
        [matchedFields],
    );
    return <HighlightContext.Provider value={value}>{children}</HighlightContext.Provider>;
};

const useHighlightContext = () => {
    return useContext(HighlightContext);
};

const useMatchedFields = () => {
    return useHighlightContext()?.matchedFields;
};

export const useHighlightedValue = (fieldName?: string) => {
    return useMatchedFields()?.find((field) => field.name === fieldName);
};

export const useIsHighlighted = () => {
    const matchedFields = useMatchedFields();

    return (fieldName: FieldName, fieldValue: string) => {
        return matchedFields?.some((field) => field.name === fieldName && field.value.includes(fieldValue));
    };
};
