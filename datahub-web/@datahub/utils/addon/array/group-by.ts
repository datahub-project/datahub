/**
 * Builds an object from the list "elements", where the value of each keyToGroupBy is a subset of values from
 * "elements" that contains an equal value for the keyToGroupBy "keyToGroupBy"
 * @template T assignable to a type Record<string, any>
 * @template V assignable to string values of T
 * @param {Array<T>} elements array of record T
 * @param {keyof T} keyToGroupBy the keyToGroupBy whose string value will be used to group the resulting map
 * @returns {Record<V, Array<T>>}
 */
export const groupBy = <T extends Record<string, any>, V extends Extract<T[keyof T], string>>(
  elements: Array<T>,
  keyToGroupBy: keyof T
): Record<V, Array<T>> =>
  elements.reduce(
    (groupMap: Record<V, Array<T>>, element: T) => {
      const groupByKey: V = element[keyToGroupBy]; // V is constrained to string by Extract in parameter declaration
      const groupValues: Array<T> = groupMap[groupByKey] || [];

      return Object.assign({}, groupMap, { [groupByKey]: [...groupValues, element] });
    },
    {} as Record<V, Array<T>>
  );
