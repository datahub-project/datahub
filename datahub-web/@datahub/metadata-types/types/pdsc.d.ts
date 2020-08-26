/**
 * Some types may be missing from the PDSC generation.
 * Using this file to fill those missing types when we don't really need
 * to import the 'real' type.
 */

// TODO META-11580: PDSC Types are not generating properly
declare namespace Com {
  namespace Linkedin {
    namespace Khronos {
      type TaskStatus = unknown;
    }
  }
}
