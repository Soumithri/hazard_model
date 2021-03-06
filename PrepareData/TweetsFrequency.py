import pickle
import networkx
from Utils.NetworkUtils import get_graphml
from Utils.Utils import get_mongo_connection, read_users, chunkIt
from Utils.Filepath import USERS, HISTORICAL_COLLECTION, HIS_FREQS
from collections import defaultdict
from multiprocessing import Pool
from functools import partial
import time

def user_freq(coll,user):
    query_string = {'user_id':user}
    print(user)
    count = 0
    for i in coll.find(query_string):
        count += 1
    return {user:count}



def historical_freq(multi = False, core = 4):
    """

    :param multi: If multiprocessing
    :param core : Core Number if multi
    :return     :
    """
    db = get_mongo_connection()
    db.his = db[HISTORICAL_COLLECTION]
    users = read_users(USERS,type="p")
    assert multi==False, 'Please set multi == False'
    if multi:
        """ Not Finished
        p = Pool(core)
        sep_users = chunkIt(users,core)
        user_freq2 = partial(user_freq,db.his)
        all_dict = p.map(user_freq2,sep_users)
        print(all_dict)
        """

    else:
        his_freqs = defaultdict(int)
        l = len(users)
        now = 0
        start = time.time()
        for user in db.his.find():
            if now % 1000 == 0:
                print('Now in %i'% now)
            if user["user_id"] in users:
                his_freqs[user["user_id"]] += 1
            now += 1

        print("Finished!")
        print('Cost %.3f' %(time.time() - start))
        pickle.dump(his_freqs,open(HIS_FREQS,"wb"))

        """ Slow
        for user in users:
            if now % 100 == 0:
                print("Now in {}".format("%i" % now))
            query_string = {'user_id':user}
            count = 0
            for i in db.his.find(query_string):
                count += 1
            his_freqs[user] = count
            now += 1
        print(his_freqs)
        """



if __name__ == "__main__":
    historical_freq(multi=False)