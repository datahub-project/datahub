# This script checks the shadow jar to ensure that we only have allowed classes being exposed through the jar
jar -tvf build/libs/datahub-client.jar |\
      grep -v "datahub/shaded" |\
      grep -v "META-INF" |\
      grep -v "com/linkedin" |\
      grep -v "com/datahub" |\
      grep -v "datahub" |\
      grep -v "entity-registry" |\
      grep -v "pegasus/" |\
      grep -v "legacyPegasusSchemas/" |\
      grep -v " com/$" |\
      grep -v "git.properties"

if [ $? -ne 0 ]; then
  echo "No other packages found. Great"
  exit 0
else
  echo "Found other packages than what we were expecting"
  exit 1
fi
