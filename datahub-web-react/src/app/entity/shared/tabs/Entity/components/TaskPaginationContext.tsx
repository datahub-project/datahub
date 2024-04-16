import React, { createContext, useContext, useState } from 'react';
import { SearchCfg } from '../../../../../../conf';

type PaginationUpdateFunction = (newCount: number, newStart: number) => void;
type TabNameUpdateFunction = (newTabName: string) => void;

interface TaskPaginationContextType {
    count: number;
    start: number;
    tabNamePagination: string | undefined;
    updateData: PaginationUpdateFunction;
    setTab: TabNameUpdateFunction;
}

const TaskPaginationContext = createContext<TaskPaginationContextType>({
    count: SearchCfg.RESULTS_PER_PAGE,
    start: 1,
    tabNamePagination: '',
    updateData: () => {},
    setTab: () => {},
});

export const useTaskPagination = () => useContext(TaskPaginationContext);

export const TaskPaginationProvider = ({ children }: { children: React.ReactNode }) => {
    const [count, setCount] = useState(SearchCfg.RESULTS_PER_PAGE);
    const [start, setStart] = useState(1);
    const [tabNamePagination, setTabName] = useState<string | undefined>('');

    const updateData: PaginationUpdateFunction = (newCount, newStart) => {
        setCount(newCount);
        setStart(newStart);
    };

    const setTab: TabNameUpdateFunction = (newTabName) => {
        setTabName(newTabName);
    };

    const contextValue: TaskPaginationContextType = {
        count,
        start,
        tabNamePagination,
        updateData,
        setTab,
    };

    return <TaskPaginationContext.Provider value={contextValue}>{children}</TaskPaginationContext.Provider>;
};
