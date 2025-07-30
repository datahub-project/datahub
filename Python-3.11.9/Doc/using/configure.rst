****************
Configure Python
****************

.. highlight:: sh

.. _configure-options:

Configure Options
=================

List all ``./configure`` script options using::

    ./configure --help

See also the :file:`Misc/SpecialBuilds.txt` in the Python source distribution.

General Options
---------------

.. option:: --enable-loadable-sqlite-extensions

   Support loadable extensions in the :mod:`!_sqlite` extension module (default
   is no) of the :mod:`sqlite3` module.

   See the :meth:`sqlite3.Connection.enable_load_extension` method of the
   :mod:`sqlite3` module.

   .. versionadded:: 3.6

.. option:: --disable-ipv6

   Disable IPv6 support (enabled by default if supported), see the
   :mod:`socket` module.

.. option:: --enable-big-digits=[15|30]

   Define the size in bits of Python :class:`int` digits: 15 or 30 bits.

   By default, the digit size is 30.

   Define the ``PYLONG_BITS_IN_DIGIT`` to ``15`` or ``30``.

   See :data:`sys.int_info.bits_per_digit <sys.int_info>`.

.. option:: --with-cxx-main
.. option:: --with-cxx-main=COMPILER

   Compile the Python ``main()`` function and link Python executable with C++
   compiler: ``$CXX``, or *COMPILER* if specified.

.. option:: --with-suffix=SUFFIX

   Set the Python executable suffix to *SUFFIX*.

   The default suffix is ``.exe`` on Windows and macOS (``python.exe``
   executable), ``.js`` on Emscripten node, ``.html`` on Emscripten browser,
   ``.wasm`` on WASI, and an empty string on other platforms (``python``
   executable).

   .. versionchanged:: 3.11
      The default suffix on WASM platform is one of ``.js``, ``.html``
      or ``.wasm``.

.. option:: --with-tzpath=<list of absolute paths separated by pathsep>

   Select the default time zone search path for :const:`zoneinfo.TZPATH`.
   See the :ref:`Compile-time configuration
   <zoneinfo_data_compile_time_config>` of the :mod:`zoneinfo` module.

   Default: ``/usr/share/zoneinfo:/usr/lib/zoneinfo:/usr/share/lib/zoneinfo:/etc/zoneinfo``.

   See :data:`os.pathsep` path separator.

   .. versionadded:: 3.9

.. option:: --without-decimal-contextvar

   Build the ``_decimal`` extension module using a thread-local context rather
   than a coroutine-local context (default), see the :mod:`decimal` module.

   See :const:`decimal.HAVE_CONTEXTVAR` and the :mod:`contextvars` module.

   .. versionadded:: 3.9

.. option:: --with-dbmliborder=<list of backend names>

   Override order to check db backends for the :mod:`dbm` module

   A valid value is a colon (``:``) separated string with the backend names:

   * ``ndbm``;
   * ``gdbm``;
   * ``bdb``.

.. option:: --without-c-locale-coercion

   Disable C locale coercion to a UTF-8 based locale (enabled by default).

   Don't define the ``PY_COERCE_C_LOCALE`` macro.

   See :envvar:`PYTHONCOERCECLOCALE` and the :pep:`538`.

.. option:: --with-platlibdir=DIRNAME

   Python library directory name (default is ``lib``).

   Fedora and SuSE use ``lib64`` on 64-bit platforms.

   See :data:`sys.platlibdir`.

   .. versionadded:: 3.9

.. option:: --with-wheel-pkg-dir=PATH

   Directory of wheel packages used by the :mod:`ensurepip` module
   (none by default).

   Some Linux distribution packaging policies recommend against bundling
   dependencies. For example, Fedora installs wheel packages in the
   ``/usr/share/python-wheels/`` directory and don't install the
   :mod:`!ensurepip._bundled` package.

   .. versionadded:: 3.10

.. option:: --with-pkg-config=[check|yes|no]

   Whether configure should use :program:`pkg-config` to detect build
   dependencies.

   * ``check`` (default): :program:`pkg-config` is optional
   * ``yes``: :program:`pkg-config` is mandatory
   * ``no``: configure does not use :program:`pkg-config` even when present

   .. versionadded:: 3.11

.. option:: --enable-pystats

   Turn on internal statistics gathering.

   The statistics will be dumped to a arbitrary (probably unique) file in
   ``/tmp/py_stats/``, or ``C:\temp\py_stats\`` on Windows.

   Use ``Tools/scripts/summarize_stats.py`` to read the stats.

   .. versionadded:: 3.11

WebAssembly Options
-------------------

.. option:: --with-emscripten-target=[browser|node]

   Set build flavor for ``wasm32-emscripten``.

   * ``browser`` (default): preload minimal stdlib, default MEMFS.
   * ``node``: NODERAWFS and pthread support.

   .. versionadded:: 3.11

.. option:: --enable-wasm-dynamic-linking

   Turn on dynamic linking support for WASM.

   Dynamic linking enables ``dlopen``. File size of the executable
   increases due to limited dead code elimination and additional features.

   .. versionadded:: 3.11

.. option:: --enable-wasm-pthreads

   Turn on pthreads support for WASM.

   .. versionadded:: 3.11


Install Options
---------------

.. option:: --prefix=PREFIX

   Install architecture-independent files in PREFIX. On Unix, it
   defaults to :file:`/usr/local`.

   This value can be retrieved at runtime using :data:`sys.prefix`.

   As an example, one can use ``--prefix="$HOME/.local/"`` to install
   a Python in its home directory.

.. option:: --exec-prefix=EPREFIX

   Install architecture-dependent files in EPREFIX, defaults to :option:`--prefix`.

   This value can be retrieved at runtime using :data:`sys.exec_prefix`.

.. option:: --disable-test-modules

   Don't build nor install test modules, like the :mod:`test` package or the
   :mod:`!_testcapi` extension module (built and installed by default).

   .. versionadded:: 3.10

.. option:: --with-ensurepip=[upgrade|install|no]

   Select the :mod:`ensurepip` command run on Python installation:

   * ``upgrade`` (default): run ``python -m ensurepip --altinstall --upgrade``
     command.
   * ``install``: run ``python -m ensurepip --altinstall`` command;
   * ``no``: don't run ensurepip;

   .. versionadded:: 3.6


Performance options
-------------------

Configuring Python using ``--enable-optimizations --with-lto`` (PGO + LTO) is
recommended for best performance.

.. option:: --enable-optimizations

   Enable Profile Guided Optimization (PGO) using :envvar:`PROFILE_TASK`
   (disabled by default).

   The C compiler Clang requires ``llvm-profdata`` program for PGO. On
   macOS, GCC also requires it: GCC is just an alias to Clang on macOS.

   Disable also semantic interposition in libpython if ``--enable-shared`` and
   GCC is used: add ``-fno-semantic-interposition`` to the compiler and linker
   flags.

   .. versionadded:: 3.6

   .. versionchanged:: 3.10
      Use ``-fno-semantic-interposition`` on GCC.

.. envvar:: PROFILE_TASK

   Environment variable used in the Makefile: Python command line arguments for
   the PGO generation task.

   Default: ``-m test --pgo --timeout=$(TESTTIMEOUT)``.

   .. versionadded:: 3.8

.. option:: --with-lto=[full|thin|no|yes]

   Enable Link Time Optimization (LTO) in any build (disabled by default).

   The C compiler Clang requires ``llvm-ar`` for LTO (``ar`` on macOS), as well
   as an LTO-aware linker (``ld.gold`` or ``lld``).

   .. versionadded:: 3.6

   .. versionadded:: 3.11
      To use ThinLTO feature, use ``--with-lto=thin`` on Clang.

.. option:: --with-computed-gotos

   Enable computed gotos in evaluation loop (enabled by default on supported
   compilers).

.. option:: --without-pymalloc

   Disable the specialized Python memory allocator :ref:`pymalloc <pymalloc>`
   (enabled by default).

   See also :envvar:`PYTHONMALLOC` environment variable.

.. option:: --without-doc-strings

   Disable static documentation strings to reduce the memory footprint (enabled
   by default). Documentation strings defined in Python are not affected.

   Don't define the ``WITH_DOC_STRINGS`` macro.

   See the ``PyDoc_STRVAR()`` macro.

.. option:: --enable-profiling

   Enable C-level code profiling with ``gprof`` (disabled by default).


.. _debug-build:

Python Debug Build
------------------

A debug build is Python built with the :option:`--with-pydebug` configure
option.

Effects of a debug build:

* Display all warnings by default: the list of default warning filters is empty
  in the :mod:`warnings` module.
* Add ``d`` to :data:`sys.abiflags`.
* Add :func:`!sys.gettotalrefcount` function.
* Add :option:`-X showrefcount <-X>` command line option.
* Add :envvar:`PYTHONTHREADDEBUG` environment variable.
* Add support for the ``__lltrace__`` variable: enable low-level tracing in the
  bytecode evaluation loop if the variable is defined.
* Install :ref:`debug hooks on memory allocators <default-memory-allocators>`
  to detect buffer overflow and other memory errors.
* Define ``Py_DEBUG`` and ``Py_REF_DEBUG`` macros.
* Add runtime checks: code surrounded by ``#ifdef Py_DEBUG`` and ``#endif``.
  Enable ``assert(...)`` and ``_PyObject_ASSERT(...)`` assertions: don't set
  the ``NDEBUG`` macro (see also the :option:`--with-assertions` configure
  option). Main runtime checks:

  * Add sanity checks on the function arguments.
  * Unicode and int objects are created with their memory filled with a pattern
    to detect usage of uninitialized objects.
  * Ensure that functions which can clear or replace the current exception are
    not called with an exception raised.
  * Check that deallocator functions don't change the current exception.
  * The garbage collector (:func:`gc.collect` function) runs some basic checks
    on objects consistency.
  * The :c:macro:`!Py_SAFE_DOWNCAST()` macro checks for integer underflow and
    overflow when downcasting from wide types to narrow types.

See also the :ref:`Python Development Mode <devmode>` and the
:option:`--with-trace-refs` configure option.

.. versionchanged:: 3.8
   Release builds and debug builds are now ABI compatible: defining the
   ``Py_DEBUG`` macro no longer implies the ``Py_TRACE_REFS`` macro (see the
   :option:`--with-trace-refs` option), which introduces the only ABI
   incompatibility.


Debug options
-------------

.. option:: --with-pydebug

   :ref:`Build Python in debug mode <debug-build>`: define the ``Py_DEBUG``
   macro (disabled by default).

.. option:: --with-trace-refs

   Enable tracing references for debugging purpose (disabled by default).

   Effects:

   * Define the ``Py_TRACE_REFS`` macro.
   * Add :func:`!sys.getobjects` function.
   * Add :envvar:`PYTHONDUMPREFS` environment variable.

   This build is not ABI compatible with release build (default build) or debug
   build (``Py_DEBUG`` and ``Py_REF_DEBUG`` macros).

   .. versionadded:: 3.8

.. option:: --with-assertions

   Build with C assertions enabled (default is no): ``assert(...);`` and
   ``_PyObject_ASSERT(...);``.

   If set, the ``NDEBUG`` macro is not defined in the :envvar:`OPT` compiler
   variable.

   See also the :option:`--with-pydebug` option (:ref:`debug build
   <debug-build>`) which also enables assertions.

   .. versionadded:: 3.6

.. option:: --with-valgrind

   Enable Valgrind support (default is no).

.. option:: --with-dtrace

   Enable DTrace support (default is no).

   See :ref:`Instrumenting CPython with DTrace and SystemTap
   <instrumentation>`.

   .. versionadded:: 3.6

.. option:: --with-address-sanitizer

   Enable AddressSanitizer memory error detector, ``asan`` (default is no).

   .. versionadded:: 3.6

.. option:: --with-memory-sanitizer

   Enable MemorySanitizer allocation error detector, ``msan`` (default is no).

   .. versionadded:: 3.6

.. option:: --with-undefined-behavior-sanitizer

   Enable UndefinedBehaviorSanitizer undefined behaviour detector, ``ubsan``
   (default is no).

   .. versionadded:: 3.6


Linker options
--------------

.. option:: --enable-shared

   Enable building a shared Python library: ``libpython`` (default is no).

.. option:: --without-static-libpython

   Do not build ``libpythonMAJOR.MINOR.a`` and do not install ``python.o``
   (built and enabled by default).

   .. versionadded:: 3.10


Libraries options
-----------------

.. option:: --with-libs='lib1 ...'

   Link against additional libraries (default is no).

.. option:: --with-system-expat

   Build the :mod:`!pyexpat` module using an installed ``expat`` library
   (default is no).

.. option:: --with-system-ffi

   Build the :mod:`_ctypes` extension module using an installed ``ffi``
   library, see the :mod:`ctypes` module (default is system-dependent).

.. option:: --with-system-libmpdec

   Build the ``_decimal`` extension module using an installed ``mpdec``
   library, see the :mod:`decimal` module (default is no).

   .. versionadded:: 3.3

.. option:: --with-readline=editline

   Use ``editline`` library for backend of the :mod:`readline` module.

   Define the ``WITH_EDITLINE`` macro.

   .. versionadded:: 3.10

.. option:: --without-readline

   Don't build the :mod:`readline` module (built by default).

   Don't define the ``HAVE_LIBREADLINE`` macro.

   .. versionadded:: 3.10

.. option:: --with-libm=STRING

   Override ``libm`` math library to *STRING* (default is system-dependent).

.. option:: --with-libc=STRING

   Override ``libc`` C library to *STRING* (default is system-dependent).

.. option:: --with-openssl=DIR

   Root of the OpenSSL directory.

   .. versionadded:: 3.7

.. option:: --with-openssl-rpath=[no|auto|DIR]

   Set runtime library directory (rpath) for OpenSSL libraries:

   * ``no`` (default): don't set rpath;
   * ``auto``: auto-detect rpath from :option:`--with-openssl` and
     ``pkg-config``;
   * *DIR*: set an explicit rpath.

   .. versionadded:: 3.10


Security Options
----------------

.. option:: --with-hash-algorithm=[fnv|siphash13|siphash24]

   Select hash algorithm for use in ``Python/pyhash.c``:

   * ``siphash13`` (default);
   * ``siphash24``;
   * ``fnv``.

   .. versionadded:: 3.4

   .. versionadded:: 3.11
      ``siphash13`` is added and it is the new default.

.. option:: --with-builtin-hashlib-hashes=md5,sha1,sha256,sha512,sha3,blake2

   Built-in hash modules:

   * ``md5``;
   * ``sha1``;
   * ``sha256``;
   * ``sha512``;
   * ``sha3`` (with shake);
   * ``blake2``.

   .. versionadded:: 3.9

.. option:: --with-ssl-default-suites=[python|openssl|STRING]

   Override the OpenSSL default cipher suites string:

   * ``python`` (default): use Python's preferred selection;
   * ``openssl``: leave OpenSSL's defaults untouched;
   * *STRING*: use a custom string

   See the :mod:`ssl` module.

   .. versionadded:: 3.7

   .. versionchanged:: 3.10

      The settings ``python`` and *STRING* also set TLS 1.2 as minimum
      protocol version.

macOS Options
-------------

See ``Mac/README.rst``.

.. option:: --enable-universalsdk
.. option:: --enable-universalsdk=SDKDIR

   Create a universal binary build. *SDKDIR* specifies which macOS SDK should
   be used to perform the build (default is no).

.. option:: --enable-framework
.. option:: --enable-framework=INSTALLDIR

   Create a Python.framework rather than a traditional Unix install. Optional
   *INSTALLDIR* specifies the installation path (default is no).

.. option:: --with-universal-archs=ARCH

   Specify the kind of universal binary that should be created. This option is
   only valid when :option:`--enable-universalsdk` is set.

   Options:

   * ``universal2``;
   * ``32-bit``;
   * ``64-bit``;
   * ``3-way``;
   * ``intel``;
   * ``intel-32``;
   * ``intel-64``;
   * ``all``.

.. option:: --with-framework-name=FRAMEWORK

   Specify the name for the python framework on macOS only valid when
   :option:`--enable-framework` is set (default: ``Python``).


Cross Compiling Options
-----------------------

Cross compiling, also known as cross building, can be used to build Python
for another CPU architecture or platform. Cross compiling requires a Python
interpreter for the build platform. The version of the build Python must match
the version of the cross compiled host Python.

.. option:: --build=BUILD

   configure for building on BUILD, usually guessed by :program:`config.guess`.

.. option:: --host=HOST

   cross-compile to build programs to run on HOST (target platform)

.. option:: --with-build-python=path/to/python

   path to build ``python`` binary for cross compiling

   .. versionadded:: 3.11

.. option:: CONFIG_SITE=file

   An environment variable that points to a file with configure overrides.

   Example *config.site* file:

   .. code-block:: ini

      # config.site-aarch64
      ac_cv_buggy_getaddrinfo=no
      ac_cv_file__dev_ptmx=yes
      ac_cv_file__dev_ptc=no


Cross compiling example::

   CONFIG_SITE=config.site-aarch64 ../configure \
       --build=x86_64-pc-linux-gnu \
       --host=aarch64-unknown-linux-gnu \
       --with-build-python=../x86_64/python


Python Build System
===================

Main files of the build system
------------------------------

* :file:`configure.ac` => :file:`configure`;
* :file:`Makefile.pre.in` => :file:`Makefile` (created by :file:`configure`);
* :file:`pyconfig.h` (created by :file:`configure`);
* :file:`Modules/Setup`: C extensions built by the Makefile using
  :file:`Module/makesetup` shell script;
* :file:`setup.py`: C extensions built using the :mod:`distutils` module.

Main build steps
----------------

* C files (``.c``) are built as object files (``.o``).
* A static ``libpython`` library (``.a``) is created from objects files.
* ``python.o`` and the static ``libpython`` library are linked into the
  final ``python`` program.
* C extensions are built by the Makefile (see :file:`Modules/Setup`)
  and ``python setup.py build``.

Main Makefile targets
---------------------

* ``make``: Build Python with the standard library.
* ``make platform:``: build the ``python`` program, but don't build the
  standard library extension modules.
* ``make profile-opt``: build Python using Profile Guided Optimization (PGO).
  You can use the configure :option:`--enable-optimizations` option to make
  this the default target of the ``make`` command (``make all`` or just
  ``make``).
* ``make buildbottest``: Build Python and run the Python test suite, the same
  way than buildbots test Python. Set ``TESTTIMEOUT`` variable (in seconds)
  to change the test timeout (1200 by default: 20 minutes).
* ``make install``: Build and install Python.
* ``make regen-all``: Regenerate (almost) all generated files;
  ``make regen-stdlib-module-names`` and ``autoconf`` must be run separately
  for the remaining generated files.
* ``make clean``: Remove built files.
* ``make distclean``: Same than ``make clean``, but remove also files created
  by the configure script.

C extensions
------------

Some C extensions are built as built-in modules, like the ``sys`` module.
They are built with the ``Py_BUILD_CORE_BUILTIN`` macro defined.
Built-in modules have no ``__file__`` attribute:

.. code-block:: pycon

    >>> import sys
    >>> sys
    <module 'sys' (built-in)>
    >>> sys.__file__
    Traceback (most recent call last):
      File "<stdin>", line 1, in <module>
    AttributeError: module 'sys' has no attribute '__file__'

Other C extensions are built as dynamic libraries, like the ``_asyncio`` module.
They are built with the ``Py_BUILD_CORE_MODULE`` macro defined.
Example on Linux x86-64:

.. code-block:: pycon

    >>> import _asyncio
    >>> _asyncio
    <module '_asyncio' from '/usr/lib64/python3.9/lib-dynload/_asyncio.cpython-39-x86_64-linux-gnu.so'>
    >>> _asyncio.__file__
    '/usr/lib64/python3.9/lib-dynload/_asyncio.cpython-39-x86_64-linux-gnu.so'

:file:`Modules/Setup` is used to generate Makefile targets to build C extensions.
At the beginning of the files, C extensions are built as built-in modules.
Extensions defined after the ``*shared*`` marker are built as dynamic libraries.

The :file:`setup.py` script only builds C extensions as shared libraries using
the :mod:`distutils` module.

The :c:macro:`!PyAPI_FUNC()`, :c:macro:`!PyAPI_DATA()` and
:c:macro:`PyMODINIT_FUNC` macros of :file:`Include/pyport.h` are defined
differently depending if the ``Py_BUILD_CORE_MODULE`` macro is defined:

* Use ``Py_EXPORTED_SYMBOL`` if the ``Py_BUILD_CORE_MODULE`` is defined
* Use ``Py_IMPORTED_SYMBOL`` otherwise.

If the ``Py_BUILD_CORE_BUILTIN`` macro is used by mistake on a C extension
built as a shared library, its :samp:`PyInit_{xxx}()` function is not exported,
causing an :exc:`ImportError` on import.


Compiler and linker flags
=========================

Options set by the ``./configure`` script and environment variables and used by
``Makefile``.

Preprocessor flags
------------------

.. envvar:: CONFIGURE_CPPFLAGS

   Value of :envvar:`CPPFLAGS` variable passed to the ``./configure`` script.

   .. versionadded:: 3.6

.. envvar:: CPPFLAGS

   (Objective) C/C++ preprocessor flags, e.g. :samp:`-I{include_dir}` if you have
   headers in a nonstandard directory *include_dir*.

   Both :envvar:`CPPFLAGS` and :envvar:`LDFLAGS` need to contain the shell's
   value for setup.py to be able to build extension modules using the
   directories specified in the environment variables.

.. envvar:: BASECPPFLAGS

   .. versionadded:: 3.4

.. envvar:: PY_CPPFLAGS

   Extra preprocessor flags added for building the interpreter object files.

   Default: ``$(BASECPPFLAGS) -I. -I$(srcdir)/Include $(CONFIGURE_CPPFLAGS) $(CPPFLAGS)``.

   .. versionadded:: 3.2

Compiler flags
--------------

.. envvar:: CC

   C compiler command.

   Example: ``gcc -pthread``.

.. envvar:: MAINCC

   C compiler command used to build the ``main()`` function of programs like
   ``python``.

   Variable set by the :option:`--with-cxx-main` option of the configure
   script.

   Default: ``$(CC)``.

.. envvar:: CXX

   C++ compiler command.

   Used if the :option:`--with-cxx-main` option is used.

   Example: ``g++ -pthread``.

.. envvar:: CFLAGS

   C compiler flags.

.. envvar:: CFLAGS_NODIST

   :envvar:`CFLAGS_NODIST` is used for building the interpreter and stdlib C
   extensions.  Use it when a compiler flag should *not* be part of the
   distutils :envvar:`CFLAGS` once Python is installed (:issue:`21121`).

   In particular, :envvar:`CFLAGS` should not contain:

   * the compiler flag ``-I`` (for setting the search path for include files).
     The ``-I`` flags are processed from left to right, and any flags in
     :envvar:`CFLAGS` would take precedence over user- and package-supplied ``-I``
     flags.

   * hardening flags such as ``-Werror`` because distributions cannot control
     whether packages installed by users conform to such heightened
     standards.

   .. versionadded:: 3.5

.. envvar:: EXTRA_CFLAGS

   Extra C compiler flags.

.. envvar:: CONFIGURE_CFLAGS

   Value of :envvar:`CFLAGS` variable passed to the ``./configure``
   script.

   .. versionadded:: 3.2

.. envvar:: CONFIGURE_CFLAGS_NODIST

   Value of :envvar:`CFLAGS_NODIST` variable passed to the ``./configure``
   script.

   .. versionadded:: 3.5

.. envvar:: BASECFLAGS

   Base compiler flags.

.. envvar:: OPT

   Optimization flags.

.. envvar:: CFLAGS_ALIASING

   Strict or non-strict aliasing flags used to compile ``Python/dtoa.c``.

   .. versionadded:: 3.7

.. envvar:: CCSHARED

   Compiler flags used to build a shared library.

   For example, ``-fPIC`` is used on Linux and on BSD.

.. envvar:: CFLAGSFORSHARED

   Extra C flags added for building the interpreter object files.

   Default: ``$(CCSHARED)`` when :option:`--enable-shared` is used, or an empty
   string otherwise.

.. envvar:: PY_CFLAGS

   Default: ``$(BASECFLAGS) $(OPT) $(CONFIGURE_CFLAGS) $(CFLAGS) $(EXTRA_CFLAGS)``.

.. envvar:: PY_CFLAGS_NODIST

   Default: ``$(CONFIGURE_CFLAGS_NODIST) $(CFLAGS_NODIST) -I$(srcdir)/Include/internal``.

   .. versionadded:: 3.5

.. envvar:: PY_STDMODULE_CFLAGS

   C flags used for building the interpreter object files.

   Default: ``$(PY_CFLAGS) $(PY_CFLAGS_NODIST) $(PY_CPPFLAGS) $(CFLAGSFORSHARED)``.

   .. versionadded:: 3.7

.. envvar:: PY_CORE_CFLAGS

   Default: ``$(PY_STDMODULE_CFLAGS) -DPy_BUILD_CORE``.

   .. versionadded:: 3.2

.. envvar:: PY_BUILTIN_MODULE_CFLAGS

   Compiler flags to build a standard library extension module as a built-in
   module, like the :mod:`posix` module.

   Default: ``$(PY_STDMODULE_CFLAGS) -DPy_BUILD_CORE_BUILTIN``.

   .. versionadded:: 3.8

.. envvar:: PURIFY

   Purify command. Purify is a memory debugger program.

   Default: empty string (not used).


Linker flags
------------

.. envvar:: LINKCC

   Linker command used to build programs like ``python`` and ``_testembed``.

   Default: ``$(PURIFY) $(MAINCC)``.

.. envvar:: CONFIGURE_LDFLAGS

   Value of :envvar:`LDFLAGS` variable passed to the ``./configure`` script.

   Avoid assigning :envvar:`CFLAGS`, :envvar:`LDFLAGS`, etc. so users can use
   them on the command line to append to these values without stomping the
   pre-set values.

   .. versionadded:: 3.2

.. envvar:: LDFLAGS_NODIST

   :envvar:`LDFLAGS_NODIST` is used in the same manner as
   :envvar:`CFLAGS_NODIST`.  Use it when a linker flag should *not* be part of
   the distutils :envvar:`LDFLAGS` once Python is installed (:issue:`35257`).

   In particular, :envvar:`LDFLAGS` should not contain:

   * the compiler flag ``-L`` (for setting the search path for libraries).
     The ``-L`` flags are processed from left to right, and any flags in
     :envvar:`LDFLAGS` would take precedence over user- and package-supplied ``-L``
     flags.

.. envvar:: CONFIGURE_LDFLAGS_NODIST

   Value of :envvar:`LDFLAGS_NODIST` variable passed to the ``./configure``
   script.

   .. versionadded:: 3.8

.. envvar:: LDFLAGS

   Linker flags, e.g. :samp:`-L{lib_dir}` if you have libraries in a nonstandard
   directory *lib_dir*.

   Both :envvar:`CPPFLAGS` and :envvar:`LDFLAGS` need to contain the shell's
   value for setup.py to be able to build extension modules using the
   directories specified in the environment variables.

.. envvar:: LIBS

   Linker flags to pass libraries to the linker when linking the Python
   executable.

   Example: ``-lrt``.

.. envvar:: LDSHARED

   Command to build a shared library.

   Default: ``@LDSHARED@ $(PY_LDFLAGS)``.

.. envvar:: BLDSHARED

   Command to build ``libpython`` shared library.

   Default: ``@BLDSHARED@ $(PY_CORE_LDFLAGS)``.

.. envvar:: PY_LDFLAGS

   Default: ``$(CONFIGURE_LDFLAGS) $(LDFLAGS)``.

.. envvar:: PY_LDFLAGS_NODIST

   Default: ``$(CONFIGURE_LDFLAGS_NODIST) $(LDFLAGS_NODIST)``.

   .. versionadded:: 3.8

.. envvar:: PY_CORE_LDFLAGS

   Linker flags used for building the interpreter object files.

   .. versionadded:: 3.8
