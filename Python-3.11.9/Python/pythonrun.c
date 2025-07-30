
/* Top level execution of Python code (including in __main__) */

/* To help control the interfaces between the startup, execution and
 * shutdown code, the phases are split across separate modules (bootstrap,
 * pythonrun, shutdown)
 */

/* TODO: Cull includes following phase split */

#include <stdbool.h>

#include "Python.h"

#include "pycore_ast.h"           // PyAST_mod2obj
#include "pycore_ceval.h"         // _Py_EnterRecursiveCall
#include "pycore_compile.h"       // _PyAST_Compile()
#include "pycore_interp.h"        // PyInterpreterState.importlib
#include "pycore_object.h"        // _PyDebug_PrintTotalRefs()
#include "pycore_parser.h"        // _PyParser_ASTFromString()
#include "pycore_pyerrors.h"      // _PyErr_Fetch, _Py_Offer_Suggestions
#include "pycore_pylifecycle.h"   // _Py_UnhandledKeyboardInterrupt
#include "pycore_pystate.h"       // _PyInterpreterState_GET()
#include "pycore_sysmodule.h"     // _PySys_Audit()
#include "pycore_traceback.h"     // _PyTraceBack_Print_Indented()

#include "token.h"                // INDENT
#include "errcode.h"              // E_EOF
#include "marshal.h"              // PyMarshal_ReadLongFromFile()

#ifdef MS_WINDOWS
#  include "malloc.h"             // alloca()
#endif

#ifdef MS_WINDOWS
#  undef BYTE
#  include "windows.h"
#endif


#ifdef __cplusplus
extern "C" {
#endif

/* Forward */
static void flush_io(void);
static PyObject *run_mod(mod_ty, PyObject *, PyObject *, PyObject *,
                          PyCompilerFlags *, PyArena *);
static PyObject *run_pyc_file(FILE *, PyObject *, PyObject *,
                              PyCompilerFlags *);
static int PyRun_InteractiveOneObjectEx(FILE *, PyObject *, PyCompilerFlags *);
static PyObject* pyrun_file(FILE *fp, PyObject *filename, int start,
                            PyObject *globals, PyObject *locals, int closeit,
                            PyCompilerFlags *flags);


int
_PyRun_AnyFileObject(FILE *fp, PyObject *filename, int closeit,
                     PyCompilerFlags *flags)
{
    int decref_filename = 0;
    if (filename == NULL) {
        filename = PyUnicode_FromString("???");
        if (filename == NULL) {
            PyErr_Print();
            return -1;
        }
        decref_filename = 1;
    }

    int res;
    if (_Py_FdIsInteractive(fp, filename)) {
        res = _PyRun_InteractiveLoopObject(fp, filename, flags);
        if (closeit) {
            fclose(fp);
        }
    }
    else {
        res = _PyRun_SimpleFileObject(fp, filename, closeit, flags);
    }

    if (decref_filename) {
        Py_DECREF(filename);
    }
    return res;
}


/* Parse input from a file and execute it */
int
PyRun_AnyFileExFlags(FILE *fp, const char *filename, int closeit,
                     PyCompilerFlags *flags)
{
    PyObject *filename_obj;
    if (filename != NULL) {
        filename_obj = PyUnicode_DecodeFSDefault(filename);
        if (filename_obj == NULL) {
            PyErr_Print();
            return -1;
        }
    }
    else {
        filename_obj = NULL;
    }
    int res = _PyRun_AnyFileObject(fp, filename_obj, closeit, flags);
    Py_XDECREF(filename_obj);
    return res;
}


int
_PyRun_InteractiveLoopObject(FILE *fp, PyObject *filename, PyCompilerFlags *flags)
{
    PyCompilerFlags local_flags = _PyCompilerFlags_INIT;
    if (flags == NULL) {
        flags = &local_flags;
    }

    PyThreadState *tstate = _PyThreadState_GET();
    PyObject *v = _PySys_GetAttr(tstate, &_Py_ID(ps1));
    if (v == NULL) {
        _PySys_SetAttr(&_Py_ID(ps1), v = PyUnicode_FromString(">>> "));
        Py_XDECREF(v);
    }
    v = _PySys_GetAttr(tstate, &_Py_ID(ps2));
    if (v == NULL) {
        _PySys_SetAttr(&_Py_ID(ps2), v = PyUnicode_FromString("... "));
        Py_XDECREF(v);
    }

#ifdef Py_REF_DEBUG
    int show_ref_count = _Py_GetConfig()->show_ref_count;
#endif
    int err = 0;
    int ret;
    int nomem_count = 0;
    do {
        ret = PyRun_InteractiveOneObjectEx(fp, filename, flags);
        if (ret == -1 && PyErr_Occurred()) {
            /* Prevent an endless loop after multiple consecutive MemoryErrors
             * while still allowing an interactive command to fail with a
             * MemoryError. */
            if (PyErr_ExceptionMatches(PyExc_MemoryError)) {
                if (++nomem_count > 16) {
                    PyErr_Clear();
                    err = -1;
                    break;
                }
            } else {
                nomem_count = 0;
            }
            PyErr_Print();
            flush_io();
        } else {
            nomem_count = 0;
        }
#ifdef Py_REF_DEBUG
        if (show_ref_count) {
            _PyDebug_PrintTotalRefs();
        }
#endif
    } while (ret != E_EOF);
    return err;
}


int
PyRun_InteractiveLoopFlags(FILE *fp, const char *filename, PyCompilerFlags *flags)
{
    PyObject *filename_obj = PyUnicode_DecodeFSDefault(filename);
    if (filename_obj == NULL) {
        PyErr_Print();
        return -1;
    }

    int err = _PyRun_InteractiveLoopObject(fp, filename_obj, flags);
    Py_DECREF(filename_obj);
    return err;

}


/* A PyRun_InteractiveOneObject() auxiliary function that does not print the
 * error on failure. */
static int
PyRun_InteractiveOneObjectEx(FILE *fp, PyObject *filename,
                             PyCompilerFlags *flags)
{
    PyObject *m, *d, *v, *w, *oenc = NULL;
    mod_ty mod;
    PyArena *arena;
    const char *ps1 = "", *ps2 = "", *enc = NULL;
    int errcode = 0;
    PyThreadState *tstate = _PyThreadState_GET();

    if (fp == stdin) {
        /* Fetch encoding from sys.stdin if possible. */
        v = _PySys_GetAttr(tstate, &_Py_ID(stdin));
        if (v && v != Py_None) {
            oenc = PyObject_GetAttr(v, &_Py_ID(encoding));
            if (oenc)
                enc = PyUnicode_AsUTF8(oenc);
            if (!enc)
                PyErr_Clear();
        }
    }
    v = _PySys_GetAttr(tstate, &_Py_ID(ps1));
    if (v != NULL) {
        v = PyObject_Str(v);
        if (v == NULL)
            PyErr_Clear();
        else if (PyUnicode_Check(v)) {
            ps1 = PyUnicode_AsUTF8(v);
            if (ps1 == NULL) {
                PyErr_Clear();
                ps1 = "";
            }
        }
    }
    w = _PySys_GetAttr(tstate, &_Py_ID(ps2));
    if (w != NULL) {
        w = PyObject_Str(w);
        if (w == NULL)
            PyErr_Clear();
        else if (PyUnicode_Check(w)) {
            ps2 = PyUnicode_AsUTF8(w);
            if (ps2 == NULL) {
                PyErr_Clear();
                ps2 = "";
            }
        }
    }
    arena = _PyArena_New();
    if (arena == NULL) {
        Py_XDECREF(v);
        Py_XDECREF(w);
        Py_XDECREF(oenc);
        return -1;
    }

    mod = _PyParser_ASTFromFile(fp, filename, enc, Py_single_input,
                                ps1, ps2, flags, &errcode, arena);

    Py_XDECREF(v);
    Py_XDECREF(w);
    Py_XDECREF(oenc);
    if (mod == NULL) {
        _PyArena_Free(arena);
        if (errcode == E_EOF) {
            PyErr_Clear();
            return E_EOF;
        }
        return -1;
    }
    m = PyImport_AddModuleObject(&_Py_ID(__main__));
    if (m == NULL) {
        _PyArena_Free(arena);
        return -1;
    }
    d = PyModule_GetDict(m);
    v = run_mod(mod, filename, d, d, flags, arena);
    _PyArena_Free(arena);
    if (v == NULL) {
        return -1;
    }
    Py_DECREF(v);
    flush_io();
    return 0;
}

int
PyRun_InteractiveOneObject(FILE *fp, PyObject *filename, PyCompilerFlags *flags)
{
    int res;

    res = PyRun_InteractiveOneObjectEx(fp, filename, flags);
    if (res == -1) {
        PyErr_Print();
        flush_io();
    }
    return res;
}

int
PyRun_InteractiveOneFlags(FILE *fp, const char *filename_str, PyCompilerFlags *flags)
{
    PyObject *filename;
    int res;

    filename = PyUnicode_DecodeFSDefault(filename_str);
    if (filename == NULL) {
        PyErr_Print();
        return -1;
    }
    res = PyRun_InteractiveOneObject(fp, filename, flags);
    Py_DECREF(filename);
    return res;
}


/* Check whether a file maybe a pyc file: Look at the extension,
   the file type, and, if we may close it, at the first few bytes. */

static int
maybe_pyc_file(FILE *fp, PyObject *filename, int closeit)
{
    PyObject *ext = PyUnicode_FromString(".pyc");
    if (ext == NULL) {
        return -1;
    }
    Py_ssize_t endswith = PyUnicode_Tailmatch(filename, ext, 0, PY_SSIZE_T_MAX, +1);
    Py_DECREF(ext);
    if (endswith) {
        return 1;
    }

    /* Only look into the file if we are allowed to close it, since
       it then should also be seekable. */
    if (!closeit) {
        return 0;
    }

    /* Read only two bytes of the magic. If the file was opened in
       text mode, the bytes 3 and 4 of the magic (\r\n) might not
       be read as they are on disk. */
    unsigned int halfmagic = PyImport_GetMagicNumber() & 0xFFFF;
    unsigned char buf[2];
    /* Mess:  In case of -x, the stream is NOT at its start now,
       and ungetc() was used to push back the first newline,
       which makes the current stream position formally undefined,
       and a x-platform nightmare.
       Unfortunately, we have no direct way to know whether -x
       was specified.  So we use a terrible hack:  if the current
       stream position is not 0, we assume -x was specified, and
       give up.  Bug 132850 on SourceForge spells out the
       hopelessness of trying anything else (fseek and ftell
       don't work predictably x-platform for text-mode files).
    */
    int ispyc = 0;
    if (ftell(fp) == 0) {
        if (fread(buf, 1, 2, fp) == 2 &&
            ((unsigned int)buf[1]<<8 | buf[0]) == halfmagic)
            ispyc = 1;
        rewind(fp);
    }
    return ispyc;
}


static int
set_main_loader(PyObject *d, PyObject *filename, const char *loader_name)
{
    PyInterpreterState *interp = _PyInterpreterState_GET();
    PyObject *bootstrap = PyObject_GetAttrString(interp->importlib,
                                                 "_bootstrap_external");
    if (bootstrap == NULL) {
        return -1;
    }

    PyObject *loader_type = PyObject_GetAttrString(bootstrap, loader_name);
    Py_DECREF(bootstrap);
    if (loader_type == NULL) {
        return -1;
    }

    PyObject *loader = PyObject_CallFunction(loader_type,
                                             "sO", "__main__", filename);
    Py_DECREF(loader_type);
    if (loader == NULL) {
        return -1;
    }

    if (PyDict_SetItemString(d, "__loader__", loader) < 0) {
        Py_DECREF(loader);
        return -1;
    }
    Py_DECREF(loader);
    return 0;
}


int
_PyRun_SimpleFileObject(FILE *fp, PyObject *filename, int closeit,
                        PyCompilerFlags *flags)
{
    PyObject *m, *d, *v;
    int set_file_name = 0, ret = -1;

    m = PyImport_AddModule("__main__");
    if (m == NULL)
        return -1;
    Py_INCREF(m);
    d = PyModule_GetDict(m);
    if (_PyDict_GetItemStringWithError(d, "__file__") == NULL) {
        if (PyErr_Occurred()) {
            goto done;
        }
        if (PyDict_SetItemString(d, "__file__", filename) < 0) {
            goto done;
        }
        if (PyDict_SetItemString(d, "__cached__", Py_None) < 0) {
            goto done;
        }
        set_file_name = 1;
    }

    int pyc = maybe_pyc_file(fp, filename, closeit);
    if (pyc < 0) {
        goto done;
    }

    if (pyc) {
        FILE *pyc_fp;
        /* Try to run a pyc file. First, re-open in binary */
        if (closeit) {
            fclose(fp);
        }

        pyc_fp = _Py_fopen_obj(filename, "rb");
        if (pyc_fp == NULL) {
            fprintf(stderr, "python: Can't reopen .pyc file\n");
            goto done;
        }

        if (set_main_loader(d, filename, "SourcelessFileLoader") < 0) {
            fprintf(stderr, "python: failed to set __main__.__loader__\n");
            ret = -1;
            fclose(pyc_fp);
            goto done;
        }
        v = run_pyc_file(pyc_fp, d, d, flags);
    } else {
        /* When running from stdin, leave __main__.__loader__ alone */
        if (PyUnicode_CompareWithASCIIString(filename, "<stdin>") != 0 &&
            set_main_loader(d, filename, "SourceFileLoader") < 0) {
            fprintf(stderr, "python: failed to set __main__.__loader__\n");
            ret = -1;
            goto done;
        }
        v = pyrun_file(fp, filename, Py_file_input, d, d,
                       closeit, flags);
    }
    flush_io();
    if (v == NULL) {
        Py_CLEAR(m);
        PyErr_Print();
        goto done;
    }
    Py_DECREF(v);
    ret = 0;
  done:
    if (set_file_name) {
        if (PyDict_DelItemString(d, "__file__")) {
            PyErr_Clear();
        }
        if (PyDict_DelItemString(d, "__cached__")) {
            PyErr_Clear();
        }
    }
    Py_XDECREF(m);
    return ret;
}


int
PyRun_SimpleFileExFlags(FILE *fp, const char *filename, int closeit,
                        PyCompilerFlags *flags)
{
    PyObject *filename_obj = PyUnicode_DecodeFSDefault(filename);
    if (filename_obj == NULL) {
        return -1;
    }
    int res = _PyRun_SimpleFileObject(fp, filename_obj, closeit, flags);
    Py_DECREF(filename_obj);
    return res;
}


int
PyRun_SimpleStringFlags(const char *command, PyCompilerFlags *flags)
{
    PyObject *m, *d, *v;
    m = PyImport_AddModule("__main__");
    if (m == NULL)
        return -1;
    d = PyModule_GetDict(m);
    v = PyRun_StringFlags(command, Py_file_input, d, d, flags);
    if (v == NULL) {
        PyErr_Print();
        return -1;
    }
    Py_DECREF(v);
    return 0;
}

static int
parse_syntax_error(PyObject *err, PyObject **message, PyObject **filename,
                   Py_ssize_t *lineno, Py_ssize_t *offset,
                   Py_ssize_t* end_lineno, Py_ssize_t* end_offset,
                   PyObject **text)
{
    Py_ssize_t hold;
    PyObject *v;

    *message = NULL;
    *filename = NULL;

    /* new style errors.  `err' is an instance */
    *message = PyObject_GetAttr(err, &_Py_ID(msg));
    if (!*message)
        goto finally;

    v = PyObject_GetAttr(err, &_Py_ID(filename));
    if (!v)
        goto finally;
    if (v == Py_None) {
        Py_DECREF(v);
        _Py_DECLARE_STR(anon_string, "<string>");
        *filename = &_Py_STR(anon_string);
        Py_INCREF(*filename);
    }
    else {
        *filename = v;
    }

    v = PyObject_GetAttr(err, &_Py_ID(lineno));
    if (!v)
        goto finally;
    hold = PyLong_AsSsize_t(v);
    Py_DECREF(v);
    if (hold < 0 && PyErr_Occurred())
        goto finally;
    *lineno = hold;

    v = PyObject_GetAttr(err, &_Py_ID(offset));
    if (!v)
        goto finally;
    if (v == Py_None) {
        *offset = -1;
        Py_DECREF(v);
    } else {
        hold = PyLong_AsSsize_t(v);
        Py_DECREF(v);
        if (hold < 0 && PyErr_Occurred())
            goto finally;
        *offset = hold;
    }

    if (Py_TYPE(err) == (PyTypeObject*)PyExc_SyntaxError) {
        v = PyObject_GetAttr(err, &_Py_ID(end_lineno));
        if (!v) {
            PyErr_Clear();
            *end_lineno = *lineno;
        }
        else if (v == Py_None) {
            *end_lineno = *lineno;
            Py_DECREF(v);
        } else {
            hold = PyLong_AsSsize_t(v);
            Py_DECREF(v);
            if (hold < 0 && PyErr_Occurred())
                goto finally;
            *end_lineno = hold;
        }

        v = PyObject_GetAttr(err, &_Py_ID(end_offset));
        if (!v) {
            PyErr_Clear();
            *end_offset = -1;
        }
        else if (v == Py_None) {
            *end_offset = -1;
            Py_DECREF(v);
        } else {
            hold = PyLong_AsSsize_t(v);
            Py_DECREF(v);
            if (hold < 0 && PyErr_Occurred())
                goto finally;
            *end_offset = hold;
        }
    } else {
        // SyntaxError subclasses
        *end_lineno = *lineno;
        *end_offset = -1;
    }

    v = PyObject_GetAttr(err, &_Py_ID(text));
    if (!v)
        goto finally;
    if (v == Py_None) {
        Py_DECREF(v);
        *text = NULL;
    }
    else {
        *text = v;
    }
    return 1;

finally:
    Py_XDECREF(*message);
    Py_XDECREF(*filename);
    return 0;
}

static int
print_error_text(PyObject *f, Py_ssize_t offset, Py_ssize_t end_offset,
                 PyObject *text_obj)
{
    size_t caret_repetitions = (end_offset > 0 && end_offset > offset) ?
                               end_offset - offset : 1;

    /* Convert text to a char pointer; return if error */
    const char *text = PyUnicode_AsUTF8(text_obj);
    if (text == NULL) {
        return -1;
    }

    /* Convert offset from 1-based to 0-based */
    offset--;

    /* Strip leading whitespace from text, adjusting offset as we go */
    while (*text == ' ' || *text == '\t' || *text == '\f') {
        text++;
        offset--;
    }

    /* Calculate text length excluding trailing newline */
    Py_ssize_t len = strlen(text);
    if (len > 0 && text[len-1] == '\n') {
        len--;
    }

    /* Clip offset to at most len */
    if (offset > len) {
        offset = len;
    }

    /* Skip past newlines embedded in text */
    for (;;) {
        const char *nl = strchr(text, '\n');
        if (nl == NULL) {
            break;
        }
        Py_ssize_t inl = nl - text;
        if (inl >= offset) {
            break;
        }
        inl += 1;
        text += inl;
        len -= inl;
        offset -= (int)inl;
    }

    /* Print text */
    if (PyFile_WriteString("    ", f) < 0) {
        return -1;
    }
    if (PyFile_WriteString(text, f) < 0) {
        return -1;
    }

    /* Make sure there's a newline at the end */
    if (text[len] != '\n') {
        if (PyFile_WriteString("\n", f) < 0) {
            return -1;
        }
    }

    /* Don't print caret if it points to the left of the text */
    if (offset < 0) {
        return 0;
    }

    /* Write caret line */
    if (PyFile_WriteString("    ", f) < 0) {
        return -1;
    }
    while (--offset >= 0) {
        if (PyFile_WriteString(" ", f) < 0) {
            return -1;
        }
    }
    for (size_t caret_iter=0; caret_iter < caret_repetitions ; caret_iter++) {
        if (PyFile_WriteString("^", f) < 0) {
            return -1;
        }
    }
    if (PyFile_WriteString("\n", f) < 0) {
        return -1;
    }
    return 0;
}


int
_Py_HandleSystemExit(int *exitcode_p)
{
    int inspect = _Py_GetConfig()->inspect;
    if (inspect) {
        /* Don't exit if -i flag was given. This flag is set to 0
         * when entering interactive mode for inspecting. */
        return 0;
    }

    if (!PyErr_ExceptionMatches(PyExc_SystemExit)) {
        return 0;
    }

    PyObject *exception, *value, *tb;
    PyErr_Fetch(&exception, &value, &tb);

    fflush(stdout);

    int exitcode = 0;
    if (value == NULL || value == Py_None) {
        goto done;
    }

    if (PyExceptionInstance_Check(value)) {
        /* The error code should be in the `code' attribute. */
        PyObject *code = PyObject_GetAttr(value, &_Py_ID(code));
        if (code) {
            Py_DECREF(value);
            value = code;
            if (value == Py_None)
                goto done;
        }
        /* If we failed to dig out the 'code' attribute,
           just let the else clause below print the error. */
    }

    if (PyLong_Check(value)) {
        exitcode = (int)PyLong_AsLong(value);
    }
    else {
        PyThreadState *tstate = _PyThreadState_GET();
        PyObject *sys_stderr = _PySys_GetAttr(tstate, &_Py_ID(stderr));
        /* We clear the exception here to avoid triggering the assertion
         * in PyObject_Str that ensures it won't silently lose exception
         * details.
         */
        PyErr_Clear();
        if (sys_stderr != NULL && sys_stderr != Py_None) {
            PyFile_WriteObject(value, sys_stderr, Py_PRINT_RAW);
        } else {
            PyObject_Print(value, stderr, Py_PRINT_RAW);
            fflush(stderr);
        }
        PySys_WriteStderr("\n");
        exitcode = 1;
    }

 done:
    /* Restore and clear the exception info, in order to properly decref
     * the exception, value, and traceback.      If we just exit instead,
     * these leak, which confuses PYTHONDUMPREFS output, and may prevent
     * some finalizers from running.
     */
    PyErr_Restore(exception, value, tb);
    PyErr_Clear();
    *exitcode_p = exitcode;
    return 1;
}


static void
handle_system_exit(void)
{
    int exitcode;
    if (_Py_HandleSystemExit(&exitcode)) {
        Py_Exit(exitcode);
    }
}


static void
_PyErr_PrintEx(PyThreadState *tstate, int set_sys_last_vars)
{
    PyObject *exception, *v, *tb, *hook;

    handle_system_exit();

    _PyErr_Fetch(tstate, &exception, &v, &tb);
    if (exception == NULL) {
        goto done;
    }

    _PyErr_NormalizeException(tstate, &exception, &v, &tb);
    if (tb == NULL) {
        tb = Py_None;
        Py_INCREF(tb);
    }
    PyException_SetTraceback(v, tb);
    if (exception == NULL) {
        goto done;
    }

    /* Now we know v != NULL too */
    if (set_sys_last_vars) {
        if (_PySys_SetAttr(&_Py_ID(last_type), exception) < 0) {
            _PyErr_Clear(tstate);
        }
        if (_PySys_SetAttr(&_Py_ID(last_value), v) < 0) {
            _PyErr_Clear(tstate);
        }
        if (_PySys_SetAttr(&_Py_ID(last_traceback), tb) < 0) {
            _PyErr_Clear(tstate);
        }
    }
    hook = _PySys_GetAttr(tstate, &_Py_ID(excepthook));
    if (_PySys_Audit(tstate, "sys.excepthook", "OOOO", hook ? hook : Py_None,
                     exception, v, tb) < 0) {
        if (PyErr_ExceptionMatches(PyExc_RuntimeError)) {
            PyErr_Clear();
            goto done;
        }
        _PyErr_WriteUnraisableMsg("in audit hook", NULL);
    }
    if (hook) {
        PyObject* stack[3];
        PyObject *result;

        stack[0] = exception;
        stack[1] = v;
        stack[2] = tb;
        result = _PyObject_FastCall(hook, stack, 3);
        if (result == NULL) {
            handle_system_exit();

            PyObject *exception2, *v2, *tb2;
            _PyErr_Fetch(tstate, &exception2, &v2, &tb2);
            _PyErr_NormalizeException(tstate, &exception2, &v2, &tb2);
            /* It should not be possible for exception2 or v2
               to be NULL. However PyErr_Display() can't
               tolerate NULLs, so just be safe. */
            if (exception2 == NULL) {
                exception2 = Py_None;
                Py_INCREF(exception2);
            }
            if (v2 == NULL) {
                v2 = Py_None;
                Py_INCREF(v2);
            }
            fflush(stdout);
            PySys_WriteStderr("Error in sys.excepthook:\n");
            PyErr_Display(exception2, v2, tb2);
            PySys_WriteStderr("\nOriginal exception was:\n");
            PyErr_Display(exception, v, tb);
            Py_DECREF(exception2);
            Py_DECREF(v2);
            Py_XDECREF(tb2);
        }
        Py_XDECREF(result);
    }
    else {
        PySys_WriteStderr("sys.excepthook is missing\n");
        PyErr_Display(exception, v, tb);
    }

done:
    Py_XDECREF(exception);
    Py_XDECREF(v);
    Py_XDECREF(tb);
}

void
_PyErr_Print(PyThreadState *tstate)
{
    _PyErr_PrintEx(tstate, 1);
}

void
PyErr_PrintEx(int set_sys_last_vars)
{
    PyThreadState *tstate = _PyThreadState_GET();
    _PyErr_PrintEx(tstate, set_sys_last_vars);
}

void
PyErr_Print(void)
{
    PyErr_PrintEx(1);
}

struct exception_print_context
{
    PyObject *file;
    PyObject *seen;            // Prevent cycles in recursion
    int exception_group_depth; // nesting level of current exception group
    bool need_close;           // Need a closing bottom frame
    int max_group_width;       // Maximum number of children of each EG
    int max_group_depth;       // Maximum nesting level of EGs
};

#define EXC_MARGIN(ctx) ((ctx)->exception_group_depth ? "| " : "")
#define EXC_INDENT(ctx) (2 * (ctx)->exception_group_depth)

static int
write_indented_margin(struct exception_print_context *ctx, PyObject *f)
{
    return _Py_WriteIndentedMargin(EXC_INDENT(ctx), EXC_MARGIN(ctx), f);
}

static int
print_exception_invalid_type(struct exception_print_context *ctx,
                             PyObject *value)
{
    PyObject *f = ctx->file;
    if (_Py_WriteIndent(EXC_INDENT(ctx), f) < 0) {
        return -1;
    }
    const char *const msg = "TypeError: print_exception(): Exception expected "
                            "for value, ";
    if (PyFile_WriteString(msg, f) < 0) {
        return -1;
    }
    if (PyFile_WriteString(Py_TYPE(value)->tp_name, f) < 0) {
        return -1;
    }
    if (PyFile_WriteString(" found\n", f) < 0) {
        return -1;
    }
    return 0;
}

static int
print_exception_traceback(struct exception_print_context *ctx, PyObject *value)
{
    PyObject *f = ctx->file;
    int err = 0;

    PyObject *tb = PyException_GetTraceback(value);
    if (tb && tb != Py_None) {
        const char *header = EXCEPTION_TB_HEADER;
        const char *header_margin = EXC_MARGIN(ctx);
        if (_PyBaseExceptionGroup_Check(value)) {
            header = EXCEPTION_GROUP_TB_HEADER;
            if (ctx->exception_group_depth == 1) {
                header_margin = "+ ";
            }
        }
        err = _PyTraceBack_Print_Indented(
            tb, EXC_INDENT(ctx), EXC_MARGIN(ctx), header_margin, header, f);
    }
    Py_XDECREF(tb);
    return err;
}

static int
print_exception_file_and_line(struct exception_print_context *ctx,
                              PyObject **value_p)
{
    PyObject *f = ctx->file;

    PyObject *tmp;
    int res = _PyObject_LookupAttr(*value_p, &_Py_ID(print_file_and_line), &tmp);
    if (res <= 0) {
        if (res < 0) {
            PyErr_Clear();
        }
        return 0;
    }
    Py_DECREF(tmp);

    PyObject *message, *filename, *text;
    Py_ssize_t lineno, offset, end_lineno, end_offset;
    if (!parse_syntax_error(*value_p, &message, &filename,
                            &lineno, &offset,
                            &end_lineno, &end_offset, &text)) {
        PyErr_Clear();
        return 0;
    }

    Py_SETREF(*value_p, message);

    PyObject *line = PyUnicode_FromFormat("  File \"%S\", line %zd\n",
                                          filename, lineno);
    Py_DECREF(filename);
    if (line == NULL) {
        goto error;
    }
    if (write_indented_margin(ctx, f) < 0) {
        goto error;
    }
    if (PyFile_WriteObject(line, f, Py_PRINT_RAW) < 0) {
        goto error;
    }
    Py_CLEAR(line);

    if (text != NULL) {
        Py_ssize_t line_size;
        const char *error_line = PyUnicode_AsUTF8AndSize(text, &line_size);
        // If the location of the error spawn multiple lines, we want
        // to just print the first one and highlight everything until
        // the end of that one since we don't support multi-line error
        // messages.
        if (end_lineno > lineno) {
            end_offset = (error_line != NULL) ? line_size : -1;
        }
        // Limit the amount of '^' that we can display to
        // the size of the text in the source line.
        if (error_line != NULL && end_offset > line_size + 1) {
            end_offset = line_size + 1;
        }
        if (print_error_text(f, offset, end_offset, text) < 0) {
            goto error;
        }
        Py_DECREF(text);
    }
    assert(!PyErr_Occurred());
    return 0;

error:
    Py_XDECREF(line);
    Py_XDECREF(text);
    return -1;
}

/* Prints the message line: module.qualname[: str(exc)] */
static int
print_exception_message(struct exception_print_context *ctx, PyObject *type,
                        PyObject *value)
{
    PyObject *f = ctx->file;

    assert(PyExceptionClass_Check(type));

    if (write_indented_margin(ctx, f) < 0) {
        return -1;
    }
    PyObject *modulename = PyObject_GetAttr(type, &_Py_ID(__module__));
    if (modulename == NULL || !PyUnicode_Check(modulename)) {
        Py_XDECREF(modulename);
        PyErr_Clear();
        if (PyFile_WriteString("<unknown>.", f) < 0) {
            return -1;
        }
    }
    else {
        if (!_PyUnicode_Equal(modulename, &_Py_ID(builtins)) &&
            !_PyUnicode_Equal(modulename, &_Py_ID(__main__)))
        {
            int res = PyFile_WriteObject(modulename, f, Py_PRINT_RAW);
            Py_DECREF(modulename);
            if (res < 0) {
                return -1;
            }
            if (PyFile_WriteString(".", f) < 0) {
                return -1;
            }
        }
        else {
            Py_DECREF(modulename);
        }
    }

    PyObject *qualname = PyType_GetQualName((PyTypeObject *)type);
    if (qualname == NULL || !PyUnicode_Check(qualname)) {
        Py_XDECREF(qualname);
        PyErr_Clear();
        if (PyFile_WriteString("<unknown>", f) < 0) {
            return -1;
        }
    }
    else {
        int res = PyFile_WriteObject(qualname, f, Py_PRINT_RAW);
        Py_DECREF(qualname);
        if (res < 0) {
            return -1;
        }
    }

    if (Py_IsNone(value)) {
        return 0;
    }

    PyObject *s = PyObject_Str(value);
    if (s == NULL) {
        PyErr_Clear();
        if (PyFile_WriteString(": <exception str() failed>", f) < 0) {
            return -1;
        }
    }
    else {
        /* only print colon if the str() of the
           object is not the empty string
        */
        if (!PyUnicode_Check(s) || PyUnicode_GetLength(s) != 0) {
            if (PyFile_WriteString(": ", f) < 0) {
                Py_DECREF(s);
                return -1;
            }
        }
        int res = PyFile_WriteObject(s, f, Py_PRINT_RAW);
        Py_DECREF(s);
        if (res < 0) {
            return -1;
        }
    }

    return 0;
}

static int
print_exception_suggestions(struct exception_print_context *ctx,
                            PyObject *value)
{
    PyObject *f = ctx->file;
    PyObject *suggestions = _Py_Offer_Suggestions(value);
    if (suggestions) {
        // Add a trailer ". Did you mean: (...)?"
        if (PyFile_WriteString(". Did you mean: '", f) < 0) {
            goto error;
        }
        if (PyFile_WriteObject(suggestions, f, Py_PRINT_RAW) < 0) {
            goto error;
        }
        if (PyFile_WriteString("'?", f) < 0) {
            goto error;
        }
        Py_DECREF(suggestions);
    }
    else if (PyErr_Occurred()) {
        PyErr_Clear();
    }
    return 0;
error:
    Py_XDECREF(suggestions);
    return -1;
}

static int
print_exception_notes(struct exception_print_context *ctx, PyObject *notes)
{
    PyObject *f = ctx->file;

    if (notes == NULL) {
        return 0;
    }

    if (!PySequence_Check(notes)) {
        int res = 0;
        if (write_indented_margin(ctx, f) < 0) {
            res = -1;
        }
        PyObject *s = PyObject_Repr(notes);
        if (s == NULL) {
            PyErr_Clear();
            res = PyFile_WriteString("<__notes__ repr() failed>", f);
        }
        else {
            res = PyFile_WriteObject(s, f, Py_PRINT_RAW);
            Py_DECREF(s);
        }
        return res;
    }
    Py_ssize_t num_notes = PySequence_Length(notes);
    PyObject *lines = NULL;
    for (Py_ssize_t ni = 0; ni < num_notes; ni++) {
        PyObject *note = PySequence_GetItem(notes, ni);
        PyObject *note_str = PyObject_Str(note);
        Py_DECREF(note);

        if (note_str == NULL) {
            PyErr_Clear();
            if (PyFile_WriteString("<note str() failed>", f) < 0) {
                goto error;
            }
        }
        else {
            lines = PyUnicode_Splitlines(note_str, 1);
            Py_DECREF(note_str);

            if (lines == NULL) {
                goto error;
            }

            Py_ssize_t n = PyList_GET_SIZE(lines);
            for (Py_ssize_t i = 0; i < n; i++) {
                PyObject *line = PyList_GET_ITEM(lines, i);
                assert(PyUnicode_Check(line));
                if (write_indented_margin(ctx, f) < 0) {
                    goto error;
                }
                if (PyFile_WriteObject(line, f, Py_PRINT_RAW) < 0) {
                    goto error;
                }
            }
            Py_CLEAR(lines);
        }
        if (PyFile_WriteString("\n", f) < 0) {
            goto error;
        }
    }

    return 0;
error:
    Py_XDECREF(lines);
    return -1;
}

static int
get_exception_notes(struct exception_print_context *ctx, PyObject *value, PyObject **notes) {
    PyObject *note = NULL;

    if (_PyObject_LookupAttr(value, &_Py_ID(__notes__), notes) < 0) {
        PyObject *type, *errvalue, *tback;
        PyErr_Fetch(&type, &errvalue, &tback);
        PyErr_NormalizeException(&type, &errvalue, &tback);
        note = PyUnicode_FromFormat("Ignored error getting __notes__: %R", errvalue);
        Py_XDECREF(type);
        Py_XDECREF(errvalue);
        Py_XDECREF(tback);
        if (!note) {
            goto error;
        }
        *notes = PyList_New(1);
        if (!*notes) {
            goto error;
        }
        if (PyList_SetItem(*notes, 0, note) < 0) {
            Py_DECREF(*notes);
            goto error;
        }
    }

    return 0;
error:
    Py_XDECREF(note);
    return -1;
}

static int
print_exception(struct exception_print_context *ctx, PyObject *value)
{
    PyObject *notes = NULL;
    PyObject *f = ctx->file;

    if (!PyExceptionInstance_Check(value)) {
        return print_exception_invalid_type(ctx, value);
    }

    Py_INCREF(value);
    fflush(stdout);

    if (print_exception_traceback(ctx, value) < 0) {
        goto error;
    }

    /* grab the type and notes now because value can change below */
    PyObject *type = (PyObject *) Py_TYPE(value);
    if (get_exception_notes(ctx, value, &notes) < 0) {
        goto error;
    }

    if (print_exception_file_and_line(ctx, &value) < 0) {
        goto error;
    }
    if (print_exception_message(ctx, type, value) < 0) {
        goto error;
    }
    if (print_exception_suggestions(ctx, value) < 0) {
        goto error;
    }
    if (PyFile_WriteString("\n", f) < 0) {
        goto error;
    }
    if (print_exception_notes(ctx, notes) < 0) {
        goto error;
    }

    Py_XDECREF(notes);
    Py_DECREF(value);
    assert(!PyErr_Occurred());
    return 0;
error:
    Py_XDECREF(notes);
    Py_DECREF(value);
    return -1;
}

static const char cause_message[] =
    "The above exception was the direct cause "
    "of the following exception:\n";

static const char context_message[] =
    "During handling of the above exception, "
    "another exception occurred:\n";

static int
print_exception_recursive(struct exception_print_context*, PyObject*);

static int
print_chained(struct exception_print_context* ctx, PyObject *value,
              const char * message, const char *tag)
{
    PyObject *f = ctx->file;

    if (_Py_EnterRecursiveCall(" in print_chained") < 0) {
        return -1;
    }
    bool need_close = ctx->need_close;
    int res = print_exception_recursive(ctx, value);
    ctx->need_close = need_close;
    _Py_LeaveRecursiveCall();
    if (res < 0) {
        return -1;
    }

    if (write_indented_margin(ctx, f) < 0) {
        return -1;
    }
    if (PyFile_WriteString("\n", f) < 0) {
        return -1;
    }
    if (write_indented_margin(ctx, f) < 0) {
        return -1;
    }
    if (PyFile_WriteString(message, f) < 0) {
        return -1;
    }
    if (write_indented_margin(ctx, f) < 0) {
        return -1;
    }
    if (PyFile_WriteString("\n", f) < 0) {
        return -1;
    }
    return 0;
}

/* Return true if value is in seen or there was a lookup error.
 * Return false if lookup succeeded and the item was not found.
 * We suppress errors because this makes us err on the side of
 * under-printing which is better than over-printing irregular
 * exceptions (e.g., unhashable ones).
 */
static bool
print_exception_seen_lookup(struct exception_print_context *ctx,
                            PyObject *value)
{
    PyObject *check_id = PyLong_FromVoidPtr(value);
    if (check_id == NULL) {
        PyErr_Clear();
        return true;
    }

    int in_seen = PySet_Contains(ctx->seen, check_id);
    Py_DECREF(check_id);
    if (in_seen == -1) {
        PyErr_Clear();
        return true;
    }

    if (in_seen == 1) {
        /* value is in seen */
        return true;
    }
    return false;
}

static int
print_exception_cause_and_context(struct exception_print_context *ctx,
                                  PyObject *value)
{
    PyObject *value_id = PyLong_FromVoidPtr(value);
    if (value_id == NULL || PySet_Add(ctx->seen, value_id) == -1) {
        PyErr_Clear();
        Py_XDECREF(value_id);
        return 0;
    }
    Py_DECREF(value_id);

    if (!PyExceptionInstance_Check(value)) {
        return 0;
    }

    PyObject *cause = PyException_GetCause(value);
    if (cause) {
        int err = 0;
        if (!print_exception_seen_lookup(ctx, cause)) {
            err = print_chained(ctx, cause, cause_message, "cause");
        }
        Py_DECREF(cause);
        return err;
    }
    if (((PyBaseExceptionObject *)value)->suppress_context) {
        return 0;
    }
    PyObject *context = PyException_GetContext(value);
    if (context) {
        int err = 0;
        if (!print_exception_seen_lookup(ctx, context)) {
            err = print_chained(ctx, context, context_message, "context");
        }
        Py_DECREF(context);
        return err;
    }
    return 0;
}

static int
print_exception_group(struct exception_print_context *ctx, PyObject *value)
{
    PyObject *f = ctx->file;

    if (ctx->exception_group_depth > ctx->max_group_depth) {
        /* depth exceeds limit */

        if (write_indented_margin(ctx, f) < 0) {
            return -1;
        }

        PyObject *line = PyUnicode_FromFormat("... (max_group_depth is %d)\n",
                                              ctx->max_group_depth);
        if (line == NULL) {
            return -1;
        }
        int err = PyFile_WriteObject(line, f, Py_PRINT_RAW);
        Py_DECREF(line);
        return err;
    }

    if (ctx->exception_group_depth == 0) {
        ctx->exception_group_depth += 1;
    }
    print_exception(ctx, value);

    PyObject *excs = ((PyBaseExceptionGroupObject *)value)->excs;
    assert(excs && PyTuple_Check(excs));
    Py_ssize_t num_excs = PyTuple_GET_SIZE(excs);
    assert(num_excs > 0);
    Py_ssize_t n;
    if (num_excs <= ctx->max_group_width) {
        n = num_excs;
    }
    else {
        n = ctx->max_group_width + 1;
    }

    ctx->need_close = false;
    for (Py_ssize_t i = 0; i < n; i++) {
        bool last_exc = (i == n - 1);
        if (last_exc) {
            // The closing frame may be added in a recursive call
            ctx->need_close = true;
        }

        if (_Py_WriteIndent(EXC_INDENT(ctx), f) < 0) {
            return -1;
        }
        bool truncated = (i >= ctx->max_group_width);
        PyObject *line;
        if (!truncated) {
            line = PyUnicode_FromFormat(
                "%s+---------------- %zd ----------------\n",
                (i == 0) ? "+-" : "  ", i + 1);
        }
        else {
            line = PyUnicode_FromFormat(
                "%s+---------------- ... ----------------\n",
                (i == 0) ? "+-" : "  ");
        }
        if (line == NULL) {
            return -1;
        }
        int err = PyFile_WriteObject(line, f, Py_PRINT_RAW);
        Py_DECREF(line);
        if (err < 0) {
            return -1;
        }

        ctx->exception_group_depth += 1;
        PyObject *exc = PyTuple_GET_ITEM(excs, i);

        if (!truncated) {
            if (_Py_EnterRecursiveCall(" in print_exception_group") != 0) {
                return -1;
            }
            int res = print_exception_recursive(ctx, exc);
            _Py_LeaveRecursiveCall();
            if (res < 0) {
                return -1;
            }
        }
        else {
            Py_ssize_t excs_remaining = num_excs - ctx->max_group_width;

            if (write_indented_margin(ctx, f) < 0) {
                return -1;
            }

            PyObject *line = PyUnicode_FromFormat(
                "and %zd more exception%s\n",
                excs_remaining, excs_remaining > 1 ? "s" : "");

            if (line == NULL) {
                return -1;
            }

            int err = PyFile_WriteObject(line, f, Py_PRINT_RAW);
            Py_DECREF(line);
            if (err < 0) {
                return -1;
            }
        }

        if (last_exc && ctx->need_close) {
            if (_Py_WriteIndent(EXC_INDENT(ctx), f) < 0) {
                return -1;
            }
            if (PyFile_WriteString(
                    "+------------------------------------\n", f) < 0) {
                return -1;
            }
            ctx->need_close = false;
        }
        ctx->exception_group_depth -= 1;
    }

    if (ctx->exception_group_depth == 1) {
        ctx->exception_group_depth -= 1;
    }
    return 0;
}

static int
print_exception_recursive(struct exception_print_context *ctx, PyObject *value)
{
    if (ctx->seen != NULL) {
        /* Exception chaining */
        if (print_exception_cause_and_context(ctx, value) < 0) {
            return -1;
        }
    }
    if (!_PyBaseExceptionGroup_Check(value)) {
        if (print_exception(ctx, value) < 0) {
            return -1;
        }
    }
    else if (print_exception_group(ctx, value) < 0) {
        return -1;
    }
    assert(!PyErr_Occurred());
    return 0;
}

#define PyErr_MAX_GROUP_WIDTH 15
#define PyErr_MAX_GROUP_DEPTH 10

void
_PyErr_Display(PyObject *file, PyObject *exception, PyObject *value, PyObject *tb)
{
    assert(file != NULL && file != Py_None);
    if (PyExceptionInstance_Check(value)
        && tb != NULL && PyTraceBack_Check(tb)) {
        /* Put the traceback on the exception, otherwise it won't get
           displayed.  See issue #18776. */
        PyObject *cur_tb = PyException_GetTraceback(value);
        if (cur_tb == NULL)
            PyException_SetTraceback(value, tb);
        else
            Py_DECREF(cur_tb);
    }

    struct exception_print_context ctx;
    ctx.file = file;
    ctx.exception_group_depth = 0;
    ctx.need_close = false;
    ctx.max_group_width = PyErr_MAX_GROUP_WIDTH;
    ctx.max_group_depth = PyErr_MAX_GROUP_DEPTH;

    /* We choose to ignore seen being possibly NULL, and report
       at least the main exception (it could be a MemoryError).
    */
    ctx.seen = PySet_New(NULL);
    if (ctx.seen == NULL) {
        PyErr_Clear();
    }
    if (print_exception_recursive(&ctx, value) < 0) {
        PyErr_Clear();
        _PyObject_Dump(value);
        fprintf(stderr, "lost sys.stderr\n");
    }
    Py_XDECREF(ctx.seen);

    /* Call file.flush() */
    PyObject *res = _PyObject_CallMethodNoArgs(file, &_Py_ID(flush));
    if (!res) {
        /* Silently ignore file.flush() error */
        PyErr_Clear();
    }
    else {
        Py_DECREF(res);
    }
}

void
PyErr_Display(PyObject *exception, PyObject *value, PyObject *tb)
{
    PyThreadState *tstate = _PyThreadState_GET();
    PyObject *file = _PySys_GetAttr(tstate, &_Py_ID(stderr));
    if (file == NULL) {
        _PyObject_Dump(value);
        fprintf(stderr, "lost sys.stderr\n");
        return;
    }
    if (file == Py_None) {
        return;
    }
    Py_INCREF(file);
    _PyErr_Display(file, exception, value, tb);
    Py_DECREF(file);
}

PyObject *
PyRun_StringFlags(const char *str, int start, PyObject *globals,
                  PyObject *locals, PyCompilerFlags *flags)
{
    PyObject *ret = NULL;
    mod_ty mod;
    PyArena *arena;

    arena = _PyArena_New();
    if (arena == NULL)
        return NULL;

    _Py_DECLARE_STR(anon_string, "<string>");
    mod = _PyParser_ASTFromString(
            str, &_Py_STR(anon_string), start, flags, arena);

    if (mod != NULL)
        ret = run_mod(mod, &_Py_STR(anon_string), globals, locals, flags, arena);
    _PyArena_Free(arena);
    return ret;
}


static PyObject *
pyrun_file(FILE *fp, PyObject *filename, int start, PyObject *globals,
           PyObject *locals, int closeit, PyCompilerFlags *flags)
{
    PyArena *arena = _PyArena_New();
    if (arena == NULL) {
        return NULL;
    }

    mod_ty mod;
    mod = _PyParser_ASTFromFile(fp, filename, NULL, start, NULL, NULL,
                                flags, NULL, arena);

    if (closeit) {
        fclose(fp);
    }

    PyObject *ret;
    if (mod != NULL) {
        ret = run_mod(mod, filename, globals, locals, flags, arena);
    }
    else {
        ret = NULL;
    }
    _PyArena_Free(arena);

    return ret;
}


PyObject *
PyRun_FileExFlags(FILE *fp, const char *filename, int start, PyObject *globals,
                  PyObject *locals, int closeit, PyCompilerFlags *flags)
{
    PyObject *filename_obj = PyUnicode_DecodeFSDefault(filename);
    if (filename_obj == NULL) {
        return NULL;
    }

    PyObject *res = pyrun_file(fp, filename_obj, start, globals,
                               locals, closeit, flags);
    Py_DECREF(filename_obj);
    return res;

}


static void
flush_io(void)
{
    PyObject *f, *r;
    PyObject *type, *value, *traceback;

    /* Save the current exception */
    PyErr_Fetch(&type, &value, &traceback);

    PyThreadState *tstate = _PyThreadState_GET();
    f = _PySys_GetAttr(tstate, &_Py_ID(stderr));
    if (f != NULL) {
        r = _PyObject_CallMethodNoArgs(f, &_Py_ID(flush));
        if (r)
            Py_DECREF(r);
        else
            PyErr_Clear();
    }
    f = _PySys_GetAttr(tstate, &_Py_ID(stdout));
    if (f != NULL) {
        r = _PyObject_CallMethodNoArgs(f, &_Py_ID(flush));
        if (r)
            Py_DECREF(r);
        else
            PyErr_Clear();
    }

    PyErr_Restore(type, value, traceback);
}

static PyObject *
run_eval_code_obj(PyThreadState *tstate, PyCodeObject *co, PyObject *globals, PyObject *locals)
{
    PyObject *v;
    /*
     * We explicitly re-initialize _Py_UnhandledKeyboardInterrupt every eval
     * _just in case_ someone is calling into an embedded Python where they
     * don't care about an uncaught KeyboardInterrupt exception (why didn't they
     * leave config.install_signal_handlers set to 0?!?) but then later call
     * Py_Main() itself (which _checks_ this flag and dies with a signal after
     * its interpreter exits).  We don't want a previous embedded interpreter's
     * uncaught exception to trigger an unexplained signal exit from a future
     * Py_Main() based one.
     */
    _Py_UnhandledKeyboardInterrupt = 0;

    /* Set globals['__builtins__'] if it doesn't exist */
    if (globals != NULL && _PyDict_GetItemStringWithError(globals, "__builtins__") == NULL) {
        if (PyErr_Occurred() ||
            PyDict_SetItemString(globals, "__builtins__",
                                 tstate->interp->builtins) < 0)
        {
            return NULL;
        }
    }

    v = PyEval_EvalCode((PyObject*)co, globals, locals);
    if (!v && _PyErr_Occurred(tstate) == PyExc_KeyboardInterrupt) {
        _Py_UnhandledKeyboardInterrupt = 1;
    }
    return v;
}

static PyObject *
run_mod(mod_ty mod, PyObject *filename, PyObject *globals, PyObject *locals,
            PyCompilerFlags *flags, PyArena *arena)
{
    PyThreadState *tstate = _PyThreadState_GET();
    PyCodeObject *co = _PyAST_Compile(mod, filename, flags, -1, arena);
    if (co == NULL)
        return NULL;

    if (_PySys_Audit(tstate, "exec", "O", co) < 0) {
        Py_DECREF(co);
        return NULL;
    }

    PyObject *v = run_eval_code_obj(tstate, co, globals, locals);
    Py_DECREF(co);
    return v;
}

static PyObject *
run_pyc_file(FILE *fp, PyObject *globals, PyObject *locals,
             PyCompilerFlags *flags)
{
    PyThreadState *tstate = _PyThreadState_GET();
    PyCodeObject *co;
    PyObject *v;
    long magic;
    long PyImport_GetMagicNumber(void);

    magic = PyMarshal_ReadLongFromFile(fp);
    if (magic != PyImport_GetMagicNumber()) {
        if (!PyErr_Occurred())
            PyErr_SetString(PyExc_RuntimeError,
                       "Bad magic number in .pyc file");
        goto error;
    }
    /* Skip the rest of the header. */
    (void) PyMarshal_ReadLongFromFile(fp);
    (void) PyMarshal_ReadLongFromFile(fp);
    (void) PyMarshal_ReadLongFromFile(fp);
    if (PyErr_Occurred()) {
        goto error;
    }
    v = PyMarshal_ReadLastObjectFromFile(fp);
    if (v == NULL || !PyCode_Check(v)) {
        Py_XDECREF(v);
        PyErr_SetString(PyExc_RuntimeError,
                   "Bad code object in .pyc file");
        goto error;
    }
    fclose(fp);
    co = (PyCodeObject *)v;
    v = run_eval_code_obj(tstate, co, globals, locals);
    if (v && flags)
        flags->cf_flags |= (co->co_flags & PyCF_MASK);
    Py_DECREF(co);
    return v;
error:
    fclose(fp);
    return NULL;
}

PyObject *
Py_CompileStringObject(const char *str, PyObject *filename, int start,
                       PyCompilerFlags *flags, int optimize)
{
    PyCodeObject *co;
    mod_ty mod;
    PyArena *arena = _PyArena_New();
    if (arena == NULL)
        return NULL;

    mod = _PyParser_ASTFromString(str, filename, start, flags, arena);
    if (mod == NULL) {
        _PyArena_Free(arena);
        return NULL;
    }
    if (flags && (flags->cf_flags & PyCF_ONLY_AST)) {
        PyObject *result = PyAST_mod2obj(mod);
        _PyArena_Free(arena);
        return result;
    }
    co = _PyAST_Compile(mod, filename, flags, optimize, arena);
    _PyArena_Free(arena);
    return (PyObject *)co;
}

PyObject *
Py_CompileStringExFlags(const char *str, const char *filename_str, int start,
                        PyCompilerFlags *flags, int optimize)
{
    PyObject *filename, *co;
    filename = PyUnicode_DecodeFSDefault(filename_str);
    if (filename == NULL)
        return NULL;
    co = Py_CompileStringObject(str, filename, start, flags, optimize);
    Py_DECREF(filename);
    return co;
}

const char *
_Py_SourceAsString(PyObject *cmd, const char *funcname, const char *what, PyCompilerFlags *cf, PyObject **cmd_copy)
{
    const char *str;
    Py_ssize_t size;
    Py_buffer view;

    *cmd_copy = NULL;
    if (PyUnicode_Check(cmd)) {
        cf->cf_flags |= PyCF_IGNORE_COOKIE;
        str = PyUnicode_AsUTF8AndSize(cmd, &size);
        if (str == NULL)
            return NULL;
    }
    else if (PyBytes_Check(cmd)) {
        str = PyBytes_AS_STRING(cmd);
        size = PyBytes_GET_SIZE(cmd);
    }
    else if (PyByteArray_Check(cmd)) {
        str = PyByteArray_AS_STRING(cmd);
        size = PyByteArray_GET_SIZE(cmd);
    }
    else if (PyObject_GetBuffer(cmd, &view, PyBUF_SIMPLE) == 0) {
        /* Copy to NUL-terminated buffer. */
        *cmd_copy = PyBytes_FromStringAndSize(
            (const char *)view.buf, view.len);
        PyBuffer_Release(&view);
        if (*cmd_copy == NULL) {
            return NULL;
        }
        str = PyBytes_AS_STRING(*cmd_copy);
        size = PyBytes_GET_SIZE(*cmd_copy);
    }
    else {
        PyErr_Format(PyExc_TypeError,
            "%s() arg 1 must be a %s object",
            funcname, what);
        return NULL;
    }

    if (strlen(str) != (size_t)size) {
        PyErr_SetString(PyExc_SyntaxError,
            "source code string cannot contain null bytes");
        Py_CLEAR(*cmd_copy);
        return NULL;
    }
    return str;
}

#if defined(USE_STACKCHECK)
#if defined(WIN32) && defined(_MSC_VER)

/* Stack checking for Microsoft C */

#include <malloc.h>
#include <excpt.h>

/*
 * Return non-zero when we run out of memory on the stack; zero otherwise.
 */
int
PyOS_CheckStack(void)
{
    __try {
        /* alloca throws a stack overflow exception if there's
           not enough space left on the stack */
        alloca(PYOS_STACK_MARGIN * sizeof(void*));
        return 0;
    } __except (GetExceptionCode() == STATUS_STACK_OVERFLOW ?
                    EXCEPTION_EXECUTE_HANDLER :
            EXCEPTION_CONTINUE_SEARCH) {
        int errcode = _resetstkoflw();
        if (errcode == 0)
        {
            Py_FatalError("Could not reset the stack!");
        }
    }
    return 1;
}

#endif /* WIN32 && _MSC_VER */

/* Alternate implementations can be added here... */

#endif /* USE_STACKCHECK */

/* Deprecated C API functions still provided for binary compatibility */

#undef PyRun_AnyFile
PyAPI_FUNC(int)
PyRun_AnyFile(FILE *fp, const char *name)
{
    return PyRun_AnyFileExFlags(fp, name, 0, NULL);
}

#undef PyRun_AnyFileEx
PyAPI_FUNC(int)
PyRun_AnyFileEx(FILE *fp, const char *name, int closeit)
{
    return PyRun_AnyFileExFlags(fp, name, closeit, NULL);
}

#undef PyRun_AnyFileFlags
PyAPI_FUNC(int)
PyRun_AnyFileFlags(FILE *fp, const char *name, PyCompilerFlags *flags)
{
    return PyRun_AnyFileExFlags(fp, name, 0, flags);
}

#undef PyRun_File
PyAPI_FUNC(PyObject *)
PyRun_File(FILE *fp, const char *p, int s, PyObject *g, PyObject *l)
{
    return PyRun_FileExFlags(fp, p, s, g, l, 0, NULL);
}

#undef PyRun_FileEx
PyAPI_FUNC(PyObject *)
PyRun_FileEx(FILE *fp, const char *p, int s, PyObject *g, PyObject *l, int c)
{
    return PyRun_FileExFlags(fp, p, s, g, l, c, NULL);
}

#undef PyRun_FileFlags
PyAPI_FUNC(PyObject *)
PyRun_FileFlags(FILE *fp, const char *p, int s, PyObject *g, PyObject *l,
                PyCompilerFlags *flags)
{
    return PyRun_FileExFlags(fp, p, s, g, l, 0, flags);
}

#undef PyRun_SimpleFile
PyAPI_FUNC(int)
PyRun_SimpleFile(FILE *f, const char *p)
{
    return PyRun_SimpleFileExFlags(f, p, 0, NULL);
}

#undef PyRun_SimpleFileEx
PyAPI_FUNC(int)
PyRun_SimpleFileEx(FILE *f, const char *p, int c)
{
    return PyRun_SimpleFileExFlags(f, p, c, NULL);
}


#undef PyRun_String
PyAPI_FUNC(PyObject *)
PyRun_String(const char *str, int s, PyObject *g, PyObject *l)
{
    return PyRun_StringFlags(str, s, g, l, NULL);
}

#undef PyRun_SimpleString
PyAPI_FUNC(int)
PyRun_SimpleString(const char *s)
{
    return PyRun_SimpleStringFlags(s, NULL);
}

#undef Py_CompileString
PyAPI_FUNC(PyObject *)
Py_CompileString(const char *str, const char *p, int s)
{
    return Py_CompileStringExFlags(str, p, s, NULL, -1);
}

#undef Py_CompileStringFlags
PyAPI_FUNC(PyObject *)
Py_CompileStringFlags(const char *str, const char *p, int s,
                      PyCompilerFlags *flags)
{
    return Py_CompileStringExFlags(str, p, s, flags, -1);
}

#undef PyRun_InteractiveOne
PyAPI_FUNC(int)
PyRun_InteractiveOne(FILE *f, const char *p)
{
    return PyRun_InteractiveOneFlags(f, p, NULL);
}

#undef PyRun_InteractiveLoop
PyAPI_FUNC(int)
PyRun_InteractiveLoop(FILE *f, const char *p)
{
    return PyRun_InteractiveLoopFlags(f, p, NULL);
}

#ifdef __cplusplus
}
#endif
