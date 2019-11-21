
run_interval=60

while [ 1 ]
do
    echo; echo; echo;
    echo "Running job with interval = $run_interval"
    #java -cp ProcessCiscoJobs.jar com.databuck.ProcessCiscoJobs
    #java -cp ProcessCiscoJobs.jar com.databuck.processCiscojobs.ProcessCiscoJobs
	java -cp .:tdgssconfig.jar:terajdbc4.jar:ProcessCiscoJobs.jar com.databuck.processCiscojobs.ProcessCiscoJobs 
    echo; echo; echo;
    echo "Watiting for $run_interval seconds "
    sleep $run_interval
done

echo " ... End of script...."
