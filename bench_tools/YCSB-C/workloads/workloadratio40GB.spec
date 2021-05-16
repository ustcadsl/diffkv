# Yahoo! Cloud System Benchmark
# Workload A: Update heavy workload
#   Application example: Session store recording recent actions
#                        
#   Read/update ratio: 50/50
#   Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
#   Request distribution: zipfian

#10GB 1KB value

recordcount=85217604
#recordcount=55000000
operationcount=85217604
workload=com.yahoo.ycsb.workloads.CoreWorkload

readallfields=true

readproportion=0
updateproportion=1
scanproportion=0
insertproportion=0

fieldlength=4072
field_len_dist=ratio
requestdistribution=zipfian
maxscanlength=3000

largevalue=0.1
midvalue=0.5
smallvalue=0.4

