dependencies on both scheduler/worker:
python3-pip
dask[complete]

dependency on scheduler/driver node:
flask

//Run dask scheduler on scheduler node:
dask-scheduler &

//Run dask workers on worker node:
dask-worker scheduler_node_ip:8786 --name 0 &
dask-worker scheduler_node_ip::8786 --name 1 &


//Start flask based application on scheduler node ( also the driver node in this case):
//Place numpy array files (0.npy and 1.npy) on worker node(s) as specified by folder name in code below
// file: daskClientAssetCorr.py
// line 14: nmp_dir = '/root/dask/numpy/'
nohup python3 daskClientAssetCorr.py &

//Test flask endpoint using curl from driver node:
//d is an array of date_id ranges
//s is input symbol_id
time curl -o a.out -d '{"d":[[10,100],[463,1057]],"s":16}'  -H 'Content-Type: application/json' -X POST http://159.69.208.232:5000/api/corr


//To create the numpy arrays from flat files use the file createNumpys.py:
stocks_per_core=628
stock_count=1255
date_count=1258
core_count=2
//Make sure above four variables are correct. Note that stocks_per_core = ceil(stock_count/core_count)


Please note that the numpy array is the partition for parallelism. 
There are as many numpy arrays as there are dask-workers.
Note that the dask-workers are named 0,1 etc. and operate on 0.npy and 1.npy respectively in this case.