# This script checks the shadow jar to ensure that we only have allowed classes being exposed through the jar
libName=datahub-client
jarishFile=$(find build/libs -name "${libName}*.jar" -exec ls -1rt "{}" +;)
jarFiles=$(echo "$jarishFile" | grep -v sources | grep -v javadoc | tail -n 1)
for jarFile in ${jarFiles}; do
jar -tvf $jarFile |\
      grep -v "datahub/shaded" |\
      grep -v "META-INF" |\
      grep -v "com/linkedin" |\
      grep -v "com/datahub" |\
      grep -v "datahub" |\
      grep -v "entity-registry" |\
      grep -v "pegasus/" |\
      grep -v "legacyPegasusSchemas/" |\
      grep -v " com/$" |\
      grep -v " org/$" |\
      grep -v " io/$" |\
      grep -v "git.properties" |\
      #grep -v "org/springframework" |\
      grep -v "org/aopalliance" |\
      grep -v "javax/" |\
      grep -v "io/swagger" |\
      grep -v "JavaSpring" |\
      grep -v "java-header-style.xml" |\
      grep -v "xml-header-style.xml" |\
      grep -v "license.header" |\
      grep -v "module-info.class" |\
      grep -v "com/google/" |\
      grep -v "org/codehaus/" |\
      grep -v "client.properties" |\
      grep -v "kafka" |\
      grep -v "win/" |\
      grep -v "include/" |\
      grep -v "linux/" |\
      grep -v "darwin" |\
      grep -v "MetadataChangeProposal.avsc" |\
      grep -v "aix"

if [ $? -ne 0 ]; then
  echo "✅ No unexpected class paths found in ${jarFile}"
else
  echo "💥 Found unexpected class paths in ${jarFile}"
  exit 1
fi
done
exit 0
