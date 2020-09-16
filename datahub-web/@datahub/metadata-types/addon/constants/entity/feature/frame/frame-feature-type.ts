/**
 * Frame feature type
 * @export
 * @enum {number}
 */
export enum FrameFeatureType {
  // Term name is the category, value is 1.0, or a real number
  Categorical = 'CATEGORICAL',
  // Term name is the category, value is 1.0, or a real number, Term vector may contains multiple values
  CategoricalSet = 'CATEGORICAL_SET',
  // Term name is a string, value is a real number, general form
  TermVector = 'TERM_VECTOR',
  // Term name is empty, value is a real number
  Numeric = 'NUMERIC',
  // Term name is index of the vector, value is a real number
  DenseVector = 'DENSE_VECTOR'
}
