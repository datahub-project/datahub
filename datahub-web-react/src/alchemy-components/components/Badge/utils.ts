/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

export const formatBadgeValue = (value: number, overflowCount?: number): string => {
    if (overflowCount === undefined || value < overflowCount) return String(value);

    return `${overflowCount}+`;
};

export function omitKeys<T extends object, K extends keyof T>(obj: T | undefined, keys: K[]): Omit<T, K> {
    const { ...rest } = obj;

    keys.forEach((key) => {
        delete rest[key];
    });

    return rest;
}
