from retweetNetwork.TweetsNetwork import TweetsNetwork
import networkx as nx
from Utils.NetworkUtils import get_graphml
import logging
from retweetNetwork.Tweet import *
import timeit
TV_SHOW = "StrangerThings"
FILE = "Tweets"  ### <- Change this to Tweets to create Node, "OLD" to create edges.
NODE_GRAPH = './StrangerThings1.graphml'
TWEET_COLL = "historical_tweets2"
OLD_COLL = "stream_tweets"

# Test for Blackish
# TV_SHOW = "Blackish"
# FILE = "Tweets"  ### <- Change this to Tweets to create Node, "OLD" to create edges.
# NODE_GRAPH = './Blackish.graphml'
# TWEET_COLL = "Blackish"
# OLD_COLL = "user_profiles"

if __name__ == "__main__":
    # Start the timer
    start_time = timeit.default_timer()
    if FILE == "Tweets":   ### 1. Create Graphml File with Node and edges by using Tweets Data
        network = TweetsNetwork(TV_SHOW)
        network.add_show(TV_SHOW)
        network.save()
    elif FILE == "OLD":    ### 2. Add edges by using Historical data.

        logging.basicConfig(filename= TV_SHOW + ".log", level=logging.INFO, format='%(asctime)s %(message)s')
        logging.info("Starting...")
        network = TweetsNetwork(TV_SHOW)                ##
        network.network = get_graphml(NODE_GRAPH)       ## Load graphml file build by tweets
        network.coll = network.db[OLD_COLL]             ## Read collections[old_tweets_2017]
        count = 0
        print(network.coll)
        print(network.network.nodes)
        for author_id in network.network.nodes():
            logging.info('Adding historical tweets for authorid:{}'.format(author_id))
            #print(author_id)
            #print(network.coll.find_one())
            query_string = {"user.id_str": str(author_id)}  ## node of networkx is string type
            #print(network.coll.find_one(query_string))
            for t in network.coll.find(query_string):
                #print(t['id_str'])
                #print(network)
                #print(Tweet(t).retweet)
                network.add_tweet(Tweet(t))
            if count % 1000 == 0:
                logging.info("Historical tweets: {} users done, {} users remain.".format(count, network.network.number_of_nodes()))
            if count % 2000 == 0:
                network.save()
            count += 1
        tweet_edges = network.network.number_of_edges()
        logging.info('Analysis historical tweets finished. {} edges added.'.format(tweet_edges))
        network.save()

    end_time = timeit.default_timer()
    total_time = (end_time-start_time)/60
    logging.info("----> Total program time taken: {0} mins".format(total_time))

