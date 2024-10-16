Command to execute:

  mvn  -Pdataflow-runner compile exec:java     -Dexec.mainClass=org.cvs.ReadKafka       -Dexec.args="--project=ggspandf \
  --gcpTempLocation=gs://df-staging-gg/temp/ \
  --region=us-east4 \
   --runner=DataflowRunner \
  --enableStreamingEngine \
 --workerMachineType=n4-standard-16 \
 --subnetwork=regions/us-east4/subnetworks/default \
  --usePublicIps=false"
