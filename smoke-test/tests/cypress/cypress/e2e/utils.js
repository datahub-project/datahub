export const hasOperationName = (req, operationName) => {
  const { body } = req;
  return (
    body.hasOwnProperty("operationName") && body.operationName === operationName
  );
};

// Alias query if operationName matches
export const aliasQuery = (req, operationName) => {
  if (hasOperationName(req, operationName)) {
    req.alias = `gql${operationName}Query`;
  }
};

export const patchResponse = (operationName, patchFunction) => {
  cy.intercept("POST", "/api/v2/graphql", (req) => {
    if (hasOperationName(req, operationName)) {
      req.reply((res) => {
        console.debug(`patchResponse ${operationName} original`, res.body.data); // eslint-disable-line no-console
        res.body.data = patchFunction(res.body.data);
        console.debug(`patchResponse ${operationName} patched`, res.body.data); // eslint-disable-line no-console
      });
    }
  });
};

/**
 * Returns a deep copy of the given object with specific values patched at the given paths.
 *
 * @param {Object} obj - The original object to patch (will not be mutated).
 * @param {Object.<string, any>} pathValueMap - An object where each key is a path (dot/bracket notation) and each value is the value to set at that path.
 *   Example: { 'a.b[0].c': 42 } will set obj.a.b[0].c = 42 in the returned copy.
 * @returns {Object} A new object with the specified paths patched with the given values.
 */
export const patchObject = (obj, pathValueMap) => {
  const copyOfObject = JSON.parse(JSON.stringify(obj));

  Object.keys(pathValueMap).forEach((path) => {
    const value = pathValueMap[path];
    const keys = path.replace(/\[(\w+)\]/g, ".$1").split(".");
    let current = copyOfObject;

    for (let i = 0; i < keys.length - 1; i += 1) {
      const key = keys[i];

      if (!(key in current)) {
        const nextKey = keys[i + 1];
        const isArray = /^\d+$/.test(nextKey);
        current[key] = isArray ? [] : {};
      }
      current = current[key];
    }

    current[keys[keys.length - 1]] = value;
  });

  return copyOfObject;
};
