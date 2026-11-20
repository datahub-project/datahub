# This script checks the shadow jar to ensure that we only have allowed classes being exposed through the jar
set -x
libName=acryl-spark-lineage
# Both published artifacts must be inspected. This used to `tail -n 1` the whole listing and check
# only the most recently written jar, which was survivable while the two differed just in their
# bundled openlineage-spark — but since #19289 they carry *separately compiled* project classes
# (Scala 2.12 vs 2.13 method descriptors), so checking one says nothing about the other. Pick the
# newest jar per Scala version rather than every match, so a stale artifact left behind by an earlier
# version bump doesn't fail the build.
scalaVersions="2.12 2.13"
jarFiles=""
for sv in ${scalaVersions}; do
  svJar=$(find build/libs -name "${libName}_${sv}-*.jar" ! -name '*-sources.jar' ! -name '*-javadoc.jar' -exec ls -1rt "{}" +)
  svJar=$(echo "$svJar" | tail -n 1)
  # Every guard below lives inside the loop, so an empty match would skip all of them and fall
  # straight through to `exit 0` — reporting success without inspecting anything. Fail before the
  # loop instead.
  if [ -z "$svJar" ]; then
    echo "💥 No ${libName}_${sv}-*.jar found under build/libs — nothing was verified."
    echo "   Refusing to exit 0 without inspecting an artifact; build the shadow jars first."
    exit 1
  fi
  jarFiles="${jarFiles} ${svJar}"
done

for jarFile in ${jarFiles}; do
  # Read the entry listing ONCE and require it to be non-empty before any guard runs. Every check
  # below decides from the emptiness of a command substitution, so an unreadable jar (missing file,
  # corrupt archive, absent `jar` binary) would make all of them look clean and the script would exit
  # 0 having verified nothing — the fail-open trap this file already fell into once.
  #
  # `set -e` is deliberately NOT used to achieve this: nearly every guard here is a
  # `var=$(... | grep ...)` whose *success* case is grep exiting 1 because it found nothing, so -e
  # would abort the script on the healthy path. `set -o pipefail` is not an option either — this
  # script has no shebang and CI runs it under dash, which rejects it outright ("Illegal option").
  jarEntries=$(jar -tf "$jarFile")
  if [ -z "$jarEntries" ]; then
    echo "💥 Could not read any entries from ${jarFile} — refusing to report success on an"
    echo "   uninspectable jar, since every check below would trivially pass."
    exit 1
  fi

  # OpenLineage must be shaded under io.acryl.shaded so this agent can coexist with environments
  # that ship their own io.openlineage.* (e.g. EMR/DataZone). Two packages MUST stay unrelocated:
  #  - io.openlineage.spark.extension: the SPI connectors implement at its canonical name.
  #  - io.openlineage.sql: JNI-backed; its native symbols are baked into the Rust .so/.dylib as
  #    Java_io_openlineage_sql_*, which shading can't rewrite. Relocating it → UnsatisfiedLinkError
  #    on JDBC/SQL parsing (issue #18558), so it must remain at its canonical name.
  unrelocatedOl=$(echo "$jarEntries" | grep '^io/openlineage/' | grep -v '^io/openlineage/spark/extension/' | grep -v '^io/openlineage/sql/' | grep -E '\.class$')
  if [ -n "$unrelocatedOl" ]; then
    echo "💥 Found unrelocated OpenLineage classes in ${jarFile}:"
    echo "$unrelocatedOl"
    exit 1
  fi

  # Positive guard for the JNI package (issue #18558): the Java class and its native libraries MUST
  # live at the canonical io/openlineage/sql/ path so the Rust-compiled Java_io_openlineage_sql_*
  # symbols resolve. If a future relocation change moves them under io/acryl/shaded/, JDBC/SQL
  # parsing crashes with UnsatisfiedLinkError — fail the build here instead of shipping it.
  sqlClass=$(echo "$jarEntries" | grep -E '^io/openlineage/sql/OpenLineageSql\.class$')
  sqlNativeLibs=$(echo "$jarEntries" | grep -E '^io/openlineage/sql/libopenlineage_sql_java.*\.(so|dylib|dll)$')
  if [ -z "$sqlClass" ] || [ -z "$sqlNativeLibs" ]; then
    echo "💥 JNI SQL parser missing at canonical io/openlineage/sql/ in ${jarFile}"
    echo "   OpenLineageSql.class present: ${sqlClass:-NO}"
    echo "   native libs present: ${sqlNativeLibs:-NO}"
    echo "   (io.openlineage.sql must be excluded from relocation — see build.gradle)"
    exit 1
  fi

  # Positive guard: the extension SPI must stay canonical so connectors implement it at its
  # canonical name — same "must not be relocated" reason as io.openlineage.sql above.
  extClasses=$(echo "$jarEntries" | grep -E '^io/openlineage/spark/extension/.*\.class$')
  if [ -z "$extClasses" ]; then
    echo "💥 Extension SPI missing at canonical io/openlineage/spark/extension/ in ${jarFile}"
    exit 1
  fi

  # The OpenLineage SQL parser's libraries are the ONLY natives this agent may ship, and they must
  # stay at the canonical io/openlineage/sql/ path (issue #18558). Any other native library means a
  # JNI-backed dependency got bundled: its symbols are compiled as Java_<canonical_package>_*, which
  # shading cannot rewrite, so relocating it yields UnsatisfiedLinkError at the first native call —
  # how snappy-java, zstd-jni, lz4-java and JNA were all silently broken. Spark supplies these, so the
  # fix is to exclude them (both the classes and the relocation), not to bundle them unrelocated.
  strayNatives=$(echo "$jarEntries" | grep -E '\.(so|dylib|dll|jnilib)$' |
      grep -vE '^io/openlineage/sql/libopenlineage_sql_java[^/]*\.(so|dylib|dll)$')
  if [ -n "$strayNatives" ]; then
    echo "💥 Found native libraries outside io/openlineage/sql/ in ${jarFile}:"
    echo "$strayNatives"
    echo "   JNI symbols cannot be relocated. Exclude the library (see the JNI notes in build.gradle)."
    exit 1
  fi

  # Our vendored OpenLineage classes (src/main/java/io/openlineage, tracked as patches under
  # patches/datahub-customizations/) must appear EXACTLY ONCE. If the dependency's copy is also
  # present at the same path the JVM resolves the last entry — the dependency's — and every DataHub
  # customization is silently inert in the shipped jar while the unshaded unit/integration suites
  # still pass. Duplicates elsewhere in the jar (dependency-vs-dependency, e.g. the antlr runtime)
  # are pre-existing and intentionally left alone.
  dupOl=$(echo "$jarEntries" | grep -E '^io/acryl/shaded/io/openlineage/.*\.class$' | sort | uniq -d)
  if [ -n "$dupOl" ]; then
    echo "💥 Vendored OpenLineage classes are duplicated in ${jarFile} (upstream copy would win):"
    echo "$dupOl"
    echo "   The JVM resolves the last jar entry, so these DataHub customizations would have no"
    echo "   effect at runtime. See vendoredOpenLineageClassPaths in build.gradle."
    exit 1
  fi

  # ...and each must be PRESENT. Rejecting duplicates alone still allows the opposite failure: if a
  # vendored class is dropped from the jar entirely (an over-broad exclude, a renamed upstream path)
  # the customization is just as absent at runtime, and the duplicate check would happily pass. Assert
  # one entry per vendored source file so both directions are covered.
  #
  # The source listing must itself be non-empty for that assertion to mean anything: if the vendored
  # tree is renamed or emptied the loop body never runs, missingOl stays empty, and this guard reports
  # success having compared nothing — the same fail-open trap the jar-listing checks above close.
  # find's stderr is deliberately NOT suppressed here, so a missing directory says so out loud.
  vendoredSrc=$(find src/main/java/io/openlineage -name '*.java')
  if [ -z "$vendoredSrc" ]; then
    echo "💥 No vendored OpenLineage sources found under src/main/java/io/openlineage."
    echo "   Refusing to report success without comparing anything. If the customizations were"
    echo "   intentionally dropped, delete this guard along with them."
    exit 1
  fi
  missingOl=""
  for olSrc in $vendoredSrc; do
    olClass="io/acryl/shaded/${olSrc#src/main/java/}"
    olClass="${olClass%.java}.class"
    echo "$jarEntries" | grep -qx "$olClass" || missingOl="${missingOl}${olClass}
"
  done
  if [ -n "$missingOl" ]; then
    echo "💥 Vendored OpenLineage classes are MISSING from ${jarFile}:"
    echo "$missingOl"
    echo "   Each file under src/main/java/io/openlineage must ship as exactly one relocated class,"
    echo "   otherwise that DataHub customization has no effect at runtime."
    exit 1
  fi

  # Guard against shading rewriting reflection class-name *string constants* (issue #19005).
  #
  # Shadow rewrites string constants in the constant pool, not just bytecode symbols. Where our code
  # identifies a class by name — Class.forName, loadClass, or comparing getCanonicalName() — and the
  # runtime supplies that class from the HOST classpath, a rewritten literal can never match what we
  # are handed, so the visitor silently stops matching and lineage is dropped with no error:
  #   - org.apache.kafka.common.TopicPartition: TopicPartitionProxy's expected-name constant was
  #     rewritten, but Spark's spark-sql-kafka-0-10 supplies the canonical class → Kafka streaming
  #     inputs were always empty.
  #   - io.github.spark_redshift_community.*: compileOnly (never bundled), so the rewritten name
  #     resolves nowhere → the Redshift relation visitor never fired.
  # Scoped to OpenLineage/DataHub classes on purpose: bundled third-party libs legitimately contain
  # rewritten self-references (a shaded Kafka client must load its own shaded serializers).
  # Note: do NOT use `strings` here — a .class file starts with 0xCAFEBABE, the same magic as a
  # Mach-O universal binary, so macOS `strings` errors out instead of scanning. `grep -a` is portable.
  hostSupplied='org\.apache\.kafka|org\.apache\.spark|org\.apache\.hadoop|io\.github\.spark_redshift_community'
  scanDir=$(mktemp -d)
  # `|| true` because unzip exits 11 when a pattern matches nothing, which is not fatal on its own.
  # What IS fatal is scanning nothing at all: an absent unzip or a failed extraction would leave an
  # empty directory, the grep below would find no matches, and this guard would report success while
  # checking nothing — the same silently-passing failure this whole check exists to prevent.
  unzip -o -q "$jarFile" 'io/acryl/shaded/io/openlineage/*' 'datahub/*' 'io/acryl/shaded/io/datahubproject/*' -d "$scanDir"
  unzipStatus=$?
  # 0 = extracted, 11 = a pattern matched nothing (harmless on its own). Any other status is a
  # real failure, and a partially-written directory must not be mistaken for a complete scan.
  if [ "$unzipStatus" -ne 0 ] && [ "$unzipStatus" -ne 11 ]; then
    echo "💥 unzip failed (exit ${unzipStatus}) extracting ${jarFile} for the reflection scan"
    rm -rf "$scanDir"
    exit 1
  fi
  scannedClasses=$(find "$scanDir" -name '*.class' | wc -l | tr -d ' ')
  if [ "$scannedClasses" -eq 0 ]; then
    echo "💥 Extracted no classes from ${jarFile} for the reflection scan (is unzip available?)."
    echo "   Refusing to pass on an empty scan — that would silently disable this guard."
    rm -rf "$scanDir"
    exit 1
  fi
  rewrittenNames=$(LC_ALL=C grep -raoE "io\.acryl\.shaded\.(${hostSupplied})[A-Za-z0-9_.\$]*" "$scanDir" | sed "s|^${scanDir}/||" | sort -u)

  # Guard the Scala binary version of the compiled classes (issue #19289).
  #
  # javac bakes the declared return type of a call into its invokevirtual descriptor, and the JVM
  # resolves methods on the full descriptor — return type included. Every Spark API returning a Seq
  # has a different descriptor per cross-build: `()Lscala/collection/Seq;` on 2.12 and
  # `()Lscala/collection/immutable/Seq;` on 2.13 (FileScanRDD.filePartitions(), UnionRDD.rdds(),
  # LogicalPlan.output(), TreeNode.children(), …). The build compiled the project sources once,
  # against Scala 2.12, and packed that same output into both jars, so on a Spark 4 / Scala 2.13
  # cluster the _2.13 agent threw NoSuchMethodError — and on the RDD path that Error escapes
  # `catch (Exception)` and Spark's tryOrStopSparkContext kills the whole application.
  #
  # Descriptors live in the constant pool as plain UTF8 strings, so a literal grep is enough — no
  # javap, no JVM start per class. `()Lscala/collection/Seq;` is the discriminator: it is emitted
  # only for these Scala-cross-built APIs. Its mirror image is NOT usable as the 2.12 signal, because
  # ScalaConversionUtils.asScalaSeqEmpty() returns immutable.Seq in *both* cross-builds.
  seq212Hits=$(LC_ALL=C grep -rlaF '()Lscala/collection/Seq;' "$scanDir" | sed "s|^${scanDir}/||" | sort -u)
  rm -rf "$scanDir"

  # Map each hit back to the source file it was compiled from, keeping only classes this project
  # owns. The scanned region also holds the relocated openlineage-spark bundle, whose own 2.12
  # classes would otherwise satisfy the positive check below without saying anything about ours.
  ownSeq212Hits=""
  for hitClass in ${seq212Hits}; do
    hitSrc="src/main/java/${hitClass#io/acryl/shaded/}"
    hitSrc="${hitSrc%%\$*}"          # inner classes compile from their outer class's source
    hitSrc="${hitSrc%.class}.java"
    if [ -f "$hitSrc" ]; then
      ownSeq212Hits="${ownSeq212Hits}${hitClass}
"
    fi
  done

  case "$jarFile" in
    *_2.13-*)
      # Asserted over the WHOLE scanned region, not just our classes: openlineage-spark_2.12 landing
      # in the 2.13 jar is the same defect one layer down, and equally invisible.
      if [ -n "$seq212Hits" ]; then
        echo "💥 Scala 2.12 method descriptors found in the Scala 2.13 artifact ${jarFile}:"
        echo "$seq212Hits"
        echo "   These classes call Spark APIs through '()Lscala/collection/Seq;', which does not"
        echo "   exist on Scala 2.13 (Spark returns scala.collection.immutable.Seq there), so every"
        echo "   such call throws NoSuchMethodError — and on the RDD path that stops the"
        echo "   SparkContext (issue #19289)."
        echo "   Fix: shadowJar_2_13 must package sourceSets.scala213.output and depend on"
        echo "   openlineage-spark_2.13 (see build.gradle)."
        exit 1
      fi
      ;;
    *_2.12-*)
      if [ -z "$ownSeq212Hits" ]; then
        echo "💥 No Scala 2.12 method descriptors found in this project's classes in ${jarFile}."
        echo "   The classes calling Spark's Seq-returning APIs must compile to"
        echo "   '()Lscala/collection/Seq;' here. Their absence means the Scala 2.13 compilation was"
        echo "   packaged into the 2.12 jar — the mirror image of issue #19289, and just as fatal on"
        echo "   a Scala 2.12 cluster."
        exit 1
      fi
      ;;
    *)
      # No fail-open: if the artifact naming ever changes, neither branch above would run and this
      # guard would quietly verify nothing — exactly the trap the rest of this script exists to avoid.
      echo "💥 Cannot tell which Scala binary version ${jarFile} is for."
      echo "   The Scala descriptor guard keys off the _2.12- / _2.13- infix in the artifact name;"
      echo "   update it alongside any change to the archiveBaseName in build.gradle."
      exit 1
      ;;
  esac

  if [ -n "$rewrittenNames" ]; then
    echo "💥 Shading rewrote reflection class-name constants for host-supplied packages in ${jarFile}:"
    echo "$rewrittenNames"
    echo "   These name classes the host classpath provides, so the rewritten value can never match"
    echo "   at runtime and the affected lineage is silently dropped (issue #19005)."
    echo "   Fix: exclude the package from relocation in build.gradle when it is never bundled, or"
    echo "   assemble the name at runtime so the relocator cannot fold it into one literal."
    exit 1
  fi

  # Anything that is neither DataHub/OpenLineage code nor a relocated dependency should not ship in
  # an agent that gets injected into a user's Spark JVM, where it can shadow the host's own copy.
  #
  # This check was inert until now: the last filter line ended in an ESCAPED pipe (`\|`), which handed
  # grep a file named "|" rather than continuing the pipeline. grep never read stdin, exited 2, and
  # the following line ran as a separate command against empty stdin — so the success branch was taken
  # unconditionally, whatever the jar contained. Two further fixes make it usable: the leftovers are
  # printed (previously they went nowhere, so a real failure was undiagnosable), and directory entries
  # are skipped, since relocation leaves empty dirs behind at the canonical paths (antlr/,
  # com/fasterxml/jackson/, …) which hold no code.
  #
  # Turning it on surfaced several pre-existing leaks, all now fixed in build.gradle rather than
  # allowlisted: slf4j-api plus a reload4j binding and a root log4j.properties (which were hijacking
  # the host's logging), commons-io (relocated, since its 51 consumers must keep the version we
  # bundle), guava's j2objc annotations, kafka-clients' protocol schemas, and a stray source file.
  # Only two entries remain below, and neither is a leak.
  unexpected=$(echo "$jarEntries" |
      grep -v '/$' |
      grep -v "log4j.xml" |
      grep -v "log4j2.xml" |
      grep -v "io/acryl/" |
      grep -v "datahub/shaded" |
      grep -v "licenses" |
      grep -v "META-INF" |
      grep -v "com/linkedin" |
      grep -v "com/datahub" |
      grep -v "datahub" |
      grep -v "entity-registry" |
      grep -v "pegasus/" |
      grep -v "legacyPegasusSchemas/" |
      grep -v "git.properties" |
      grep -v "org/aopalliance" |
      grep -v "javax/" |
      grep -v "jakarta/" |
      grep -v "JavaSpring" |
      grep -v "java-header-style.xml" |
      grep -v "xml-header-style.xml" |
      grep -v "license.header" |
      grep -v "module-info.class" |
      grep -v "client.properties" |
      # NOTE: a blanket `grep -v "kafka"` used to sit here. Kafka is relocated, so it hid nothing
      # legitimate beyond the one resource below — while silently excusing exactly the unrelocated
      # Kafka collision this check should catch. Same for a stale "org/apache/log4j" entry, now
      # removed: log4j is relocated and the canonical path is empty.
      grep -v "^kafka/kafka-version.properties$" |   # kafka-clients version stamp, read by
                                                     # AppInfoParser from an unrelocatable path
      grep -v "win/" |
      grep -v "include/" |
      grep -v "linux/" |
      grep -v "darwin" |
      grep -v "aix" |
      grep -v "MetadataChangeProposal.avsc" |
      # Anchored rather than the old unanchored "io.openlineage": '.' matches '/', so that
      # pattern also exempted any future non-sql/extension OpenLineage leak. These two paths
      # are the only ones that legitimately stay canonical (both asserted present above).
      grep -v '^io/openlineage/sql/' |
      grep -v '^io/openlineage/spark/extension/' |
      grep -v "library.properties" |
      grep -v "rootdoc.txt" |
      grep -v "com/ibm/" |
      # --- Deliberately retained, not leaks. Do NOT extend this list to silence a new leak: fix the
      # --- packaging in build.gradle instead.
      grep -v "^mime.types$" |                # also a legitimate shaded AWS SDK resource; a basename
                                              # exclude would strip that copy too
      grep -v "^LICENSE-ClassGraph.txt$")     # attribution file, should ship

  if [ -n "$unexpected" ]; then
    echo "💥 Found unexpected class paths in ${jarFile}:"
    echo "$unexpected"
    echo "   These ship unrelocated inside an agent injected into the user's Spark JVM and can shadow"
    echo "   Spark's own copies. Relocate or exclude them in build.gradle."
    exit 1
  fi
  echo "✅ No unexpected class paths found in ${jarFile}"
done
exit 0
