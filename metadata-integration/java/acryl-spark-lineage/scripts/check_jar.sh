# This script checks the shadow jar to ensure that we only have allowed classes being exposed through the jar
set -x
libName=acryl-spark-lineage
jarishFile=$(find build/libs -name "${libName}*.jar" -exec ls -1rt "{}" +;)
jarFiles=$(echo "$jarishFile" | grep -v sources | grep -v javadoc | tail -n 1)
for jarFile in ${jarFiles}; do
  # OpenLineage must be shaded under io.acryl.shaded so this agent can coexist with environments
  # that ship their own io.openlineage.* (e.g. EMR/DataZone). The only OpenLineage classes allowed
  # to remain unrelocated are the extension SPI, which connectors implement at its canonical name.
  unrelocatedOl=$(jar -tf "$jarFile" | grep '^io/openlineage/' | grep -v '^io/openlineage/spark/extension/' | grep -E '\.class$')
  if [ -n "$unrelocatedOl" ]; then
    echo "💥 Found unrelocated OpenLineage classes in ${jarFile}:"
    echo "$unrelocatedOl"
    exit 1
  fi

  jar -tvf $jarFile |\
      grep -v "/$" |\
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
      grep -v "org.apache" |\
      grep -v "aix" |\
      grep -v "io/micrometer/" |\
      grep -v "org/slf4j" |\
      grep -v "common/message" |\
      grep -v "freebsd/" |\
      grep -v "mime.types" |\
      grep -v "VersionInfo.java" |\
      grep -v "log4j.properties" |\
      grep -v "LICENSE" |\
      grep -E -v "library.properties|rootdoc.txt" |\
      grep -v "com/ibm/.*" |\
      grep -v "org/publicsuffix"


if [ $? -ne 0 ]; then
  echo "✅ No unexpected class paths found in ${jarFile}"
else
  echo "💥 Found unexpected class paths in ${jarFile}"
  exit 1
fi
done
exit 0
