# This script checks the shadow jar to ensure that we only have allowed classes being exposed through the jar
set -x
libName=acryl-spark-lineage
jarishFile=$(find build/libs -name "${libName}*.jar" -exec ls -1rt "{}" +;)
jarFiles=$(echo "$jarishFile" | grep -v sources | grep -v javadoc | tail -n 1)
for jarFile in ${jarFiles}; do
  # OpenLineage must be shaded under io.acryl.shaded so this agent can coexist with environments
  # that ship their own io.openlineage.* (e.g. EMR/DataZone). Two packages MUST stay unrelocated:
  #  - io.openlineage.spark.extension: the SPI connectors implement at its canonical name.
  #  - io.openlineage.sql: JNI-backed; its native symbols are baked into the Rust .so/.dylib as
  #    Java_io_openlineage_sql_*, which shading can't rewrite. Relocating it → UnsatisfiedLinkError
  #    on JDBC/SQL parsing (issue #18558), so it must remain at its canonical name.
  unrelocatedOl=$(jar -tf "$jarFile" | grep '^io/openlineage/' | grep -v '^io/openlineage/spark/extension/' | grep -v '^io/openlineage/sql/' | grep -E '\.class$')
  if [ -n "$unrelocatedOl" ]; then
    echo "💥 Found unrelocated OpenLineage classes in ${jarFile}:"
    echo "$unrelocatedOl"
    exit 1
  fi

  # Positive guard for the JNI package (issue #18558): the Java class and its native libraries MUST
  # live at the canonical io/openlineage/sql/ path so the Rust-compiled Java_io_openlineage_sql_*
  # symbols resolve. If a future relocation change moves them under io/acryl/shaded/, JDBC/SQL
  # parsing crashes with UnsatisfiedLinkError — fail the build here instead of shipping it.
  sqlClass=$(jar -tf "$jarFile" | grep -E '^io/openlineage/sql/OpenLineageSql\.class$')
  sqlNativeLibs=$(jar -tf "$jarFile" | grep -E '^io/openlineage/sql/libopenlineage_sql_java.*\.(so|dylib|dll)$')
  if [ -z "$sqlClass" ] || [ -z "$sqlNativeLibs" ]; then
    echo "💥 JNI SQL parser missing at canonical io/openlineage/sql/ in ${jarFile}"
    echo "   OpenLineageSql.class present: ${sqlClass:-NO}"
    echo "   native libs present: ${sqlNativeLibs:-NO}"
    echo "   (io.openlineage.sql must be excluded from relocation — see build.gradle)"
    exit 1
  fi

  jar -tvf $jarFile |\
      grep -v "log4j.xml" |\
      grep -v "log4j2.xml" |\
      grep -v "org/apache/log4j" |\
      grep -v "io/acryl/" |\
      grep -v "datahub/shaded" |\
      grep -v "licenses" |\
      grep -v "META-INF" |\
      grep -v "com/linkedin" |\
      grep -v "com/datahub" |\
      grep -v "datahub" |\
      grep -v "entity-registry" |\
      grep -v "pegasus/" |\
      grep -v "legacyPegasusSchemas/" |\
      grep -v " com/$" |\
      grep -v "git.properties" |\
      grep -v " org/$" |\
      grep -v " io/$" |\
      grep -v "git.properties" |\
      grep -v "org/aopalliance" |\
      grep -v "javax/" |\
      grep -v "jakarta/" |\
      grep -v "JavaSpring" |\
      grep -v "java-header-style.xml" |\
      grep -v "xml-header-style.xml" |\
      grep -v "license.header" |\
      grep -v "module-info.class" |\
      grep -v "client.properties" |\
      grep -v "kafka" |\
      grep -v "win/" |\
      grep -v "include/" |\
      grep -v "linux/" |\
      grep -v "darwin" |\
      grep -v "aix" |\
      grep -v "MetadataChangeProposal.avsc" |\
      grep -v "io.openlineage" |\
      grep -v "library.properties|rootdoc.txt" \|
      grep -v "com/ibm/.*"


if [ $? -ne 0 ]; then
  echo "✅ No unexpected class paths found in ${jarFile}"
else
  echo "💥 Found unexpected class paths in ${jarFile}"
  exit 1
fi
done
exit 0
