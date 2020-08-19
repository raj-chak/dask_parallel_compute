from dask.distributed import get_worker
from dask.distributed import Client
import json
import numpy as np
import itertools
import asyncio
from flask import json,jsonify
from flask import Flask, request

app = Flask(__name__)

class RemoteWorker:
    import numpy as np
    nmp_dir = '/root/dask/numpy/'
    mmapped_arr = {}
    my_core_id = -1

    def __init__(self):
        return

    def ping(self,num_cores):
        w = get_worker()
        self.my_core_id = w.name
        for k in range(num_cores):
            self.mmapped_arr[str(k)] = self.np.load(self.nmp_dir + str(k) + '.npy', mmap_mode='r')
        return w.name
 
    def Pearsons(self, X, Y):
        Y = Y.reshape((Y.size, 1))
        (n, t) = X.shape
        (n_1, m) = Y.shape
        DX = X - (self.np.sum(X, 0) / self.np.double(n))
        DY = Y - (self.np.sum(Y, 0) / self.np.double(n))
        co_var = self.np.einsum("nt,nm->tm", DX, DY)
        varX = self.np.sum(DX ** 2, 0)
        varY = self.np.sum(DY ** 2, 0)
        return self.np.nan_to_num((co_var / self.np.sqrt(self.np.outer(varX, varY))), copy=False).flatten()


    def corr(self, date_ids, core_symbol):
        all_symbols_arr = self.mmapped_arr[self.my_core_id][date_ids]
        input_symbol_arr = self.mmapped_arr[str(core_symbol[0])][:, core_symbol[1]][date_ids]
        return self.Pearsons(all_symbols_arr, input_symbol_arr)


symbols_per_core = 628
num_cores = 2
acts = []
c = Client('127.0.0.1:8786', direct_to_workers=True)
for i in range(num_cores):
    ws = ''+str(i)
    ftr = c.submit(RemoteWorker, workers=ws, actor=True)
    a = ftr.result()
    acts.append(a)

print(acts)

wns = []
for i in range(num_cores):
    wn = acts[i].ping(num_cores)
    wns.append(wn)

print([wn.result() for wn in wns])

async def fetch(actor_ftr):
    return actor_ftr.result().tolist()
  
def get_core_symbol_tuple(symbol_id):
    core_id = int(symbol_id/symbols_per_core)
    return (core_id, symbol_id - (core_id*symbols_per_core))


def getDateIdsforRanges(a):
    return list(set(itertools.chain.from_iterable([range(ele[0],ele[1]+1) for ele in a])))


@app.route('/api/corr', methods=['POST'])
def corr():
    content = request.get_json()
    z = getDateIdsforRanges(content['d'])
    wns = []
    (core_id,symbol_id)=get_core_symbol_tuple(content['s'])
    for i in range(num_cores):
        wns.append(asyncio.ensure_future(fetch(acts[i].corr(z,(core_id,symbol_id)))))

    loop = asyncio.get_event_loop()
    responses = jsonify(loop.run_until_complete(asyncio.gather(*wns)))
    loop.close()
    return responses
 
if __name__ == '__main__':
    app.run(host= '127.0.0.1')