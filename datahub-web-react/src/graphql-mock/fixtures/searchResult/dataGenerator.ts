/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { AnyRecord } from '@graphql-mock/types';

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
