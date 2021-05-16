# Yahoo! Cloud System Benchmark
# Workload A: Update heavy workload
#   Application example: Session store recording recent actions
#                        
#   Read/update ratio: 50/50
#   Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
#   Request distribution: zipfian

#10GB 1KB value

recordcount=268883595
#recordcount=30000000
#operationcount=284058682
operationcount=30000000
workload=com.yahoo.ycsb.workloads.CoreWorkload

readallfields=true

readproportion=0.95
updateproportion=0.05
scanproportion=0
insertproportion=0

fieldlength=4072
field_len_dist=ratio
requestdistribution=uniform
scanlengthdistribution=constant
maxscanlength=20

largeproportion=0.05
midproportion=0.55
smallproportion=0.4
largesize=32768
midsize=1024
smallsize=100
