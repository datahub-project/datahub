prev=700
datahub ingest -c hive.yaml

for i in 800 900 1000 1100 1200 1300 1400 1500 1600 1700 1800 1900 2000 2100 2200 2300 2400 2500 2600 2700
do
   echo "Welcome $i times"
   # echo "s/${prev}/${i}/"
   sed -i "" "s/${prev}/${i}/" hive.yaml
   prev=$i
   datahub ingest -c hive.yaml
done

# sed -i '' 's/100/200/' hive.yaml
