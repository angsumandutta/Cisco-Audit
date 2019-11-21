
sourceTable=$1
targetTable=$2
loadType=$3

echo "...START : $sourceTable starting ..."
#java -cp ExecuteCiscoJobs.jar com.databuck.ExecuteCiscoJobs.Application $sourceTable $targetTable $loadType > ./logs/$sourceTable
#java -cp ExecuteCiscoJobs.jar com.databuck.ExecuteCiscoJobs.Application $sourceTable $targetTable $loadType
java -cp .:tdgssconfig.jar:terajdbc4.jar:ProcessCiscoJobs.jar com.databuck.ExecuteCiscoJobs.Application $sourceTable $targetTable $loadType > ./logs/$sourceTable

echo "...END : $sourceTable completed ..."
