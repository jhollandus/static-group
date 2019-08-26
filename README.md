# Stubborn Leader or "The Geezer"

The Geezer is a way to statically assign consumers within a consumer
group and eliminate costly rebalances. Certain situations call for a
leader like this namely:

 * Large consumer groups where a rebalance requires the corrdination of
   possibly dozens of members, maybe 100+
 * The group dynamically scales based on volumes and is continually
   scaling up and down triggering many rebalance events
 * A highly active deployment pipeline is in place triggering entire
   rebalance events whenever changes are pushed, maybe several times a
day
 * Finally a combination of all of the above

What the geezer is trying to do is get its group members to spend less
time in meetings and more time getting stuff done. A side effect of his
obstinate attitude is that latencies are decreased, throughput is
increased but time to react to a lost member may be increased.





