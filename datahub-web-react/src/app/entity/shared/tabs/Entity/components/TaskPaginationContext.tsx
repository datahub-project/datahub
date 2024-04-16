import React, { createContext, useContext, useState } from 'react';
import { SearchCfg } from '../../../../../../conf';

type PaginationUpdateFunction = (newCount: number, newStart: number) => void;

const TaskPaginationContext = createContext({
    count: SearchCfg.RESULTS_PER_PAGE,
    start: 1,
    updateData: (() => {}) as PaginationUpdateFunction,
});

export const useTaskPagination = () => useContext(TaskPaginationContext);

export const TaskPaginationProvider = ({ children }: { children: React.ReactNode }) => {
    const [count, setCount] = useState(SearchCfg.RESULTS_PER_PAGE);
    const [start, setStart] = useState(1);

    const updateData: PaginationUpdateFunction = (newCount, newStart) => {
        setCount(newCount);
        setStart(newStart);
    };

    const contextValue = {
        count,
        start,
        updateData,
    };

    return <TaskPaginationContext.Provider value={contextValue}>{children}</TaskPaginationContext.Provider>;
};
