import { AnyRecord } from '../../types';

type GenerateDataArg<T = AnyRecord> = {
    generator(): T;
    count: number;
};

export const times = (count: number) => {
    return Array.from(new Array(count));
};

export const generateData = <T = AnyRecord>({ generator, count }: GenerateDataArg<T>): T[] => {
    return times(count).map(() => {
        return generator();
    });
};
