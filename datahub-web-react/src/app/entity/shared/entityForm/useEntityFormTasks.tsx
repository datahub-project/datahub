import { GetTestResultsSummaryQuery, useGetTestResultsSummaryLazyQuery } from '@src/graphql/test.generated';
import { Test } from '@src/types.generated';
import { useEffect, useState } from 'react';

const ACTIVE_TASKS_KEY = 'activeTaks';

export function useEntityFormTasks(formUrn: string) {
    const localStorageTasksKey = `${formUrn}-${ACTIVE_TASKS_KEY}`;
    const [activeTasks, setActiveTasks] = useState<Test[]>([]);
    const [completeTasks, setCompleteTasks] = useState<Test[]>([]);

    const [fetchTask] = useGetTestResultsSummaryLazyQuery({
        onCompleted: (data: GetTestResultsSummaryQuery) => {
            if (data.test?.results.lastRunTimestampMillis) {
                // task is complete
                const taskUrn = data.test.urn;
                removeFromLocalStorage(taskUrn);
                setActiveTasks(activeTasks.filter((t) => t.urn !== taskUrn));
                setCompleteTasks([data.test as Test, ...completeTasks]);
            } else if (data.test) {
                if (!activeTasks.find((t) => t.urn === data.test?.urn)) {
                    setActiveTasks([data.test as Test, ...activeTasks]);
                }
                setTimeout(() => fetchTask({ variables: { urn: data.test?.urn || '' } }), 3000);
            }
        },
    });

    function removeFromLocalStorage(taskUrn: string) {
        const tasksInLocalStorage: string[] = JSON.parse(localStorage.getItem(localStorageTasksKey) || '[]');
        localStorage.setItem(localStorageTasksKey, JSON.stringify(tasksInLocalStorage.filter((t) => t !== taskUrn)));
    }

    function handleAsyncBatchSubmit(taskUrn: string) {
        const tasksInLocalStorage: string[] = JSON.parse(localStorage.getItem(localStorageTasksKey) || '[]');
        localStorage.setItem(localStorageTasksKey, JSON.stringify([...tasksInLocalStorage, taskUrn]));
        fetchTask({ variables: { urn: taskUrn } });
    }

    useEffect(() => {
        const tasksInLocalStorage = JSON.parse(localStorage.getItem(localStorageTasksKey) || '[]');
        tasksInLocalStorage.forEach((task) => fetchTask({ variables: { urn: task } }));
    }, [fetchTask, localStorageTasksKey]);

    return { activeTasks, completeTasks, handleAsyncBatchSubmit };
}
