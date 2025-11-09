1. For replication we send Op(Payload) and removes + adds (adds are removes!) send the dots being removed. This means in the receive buffer it is not just the dot of the op we consider but also the context dots, since if the dot is (a, 3) and the ctx are (a,1), (a,2) then we can apply this, skipping the adds we never saw.



1. Implement local operations (add, rem, card, get, etc)
2. Add replication
3. Add RBILT
4. Add a stateful proptest that uses a full state replicated AddWins Set as a model for the system

But let's just start with 1. Implementing all the sql operations for the server

Also, I want to do some kind of benchmark (basho_bench?? to compare to the original bigsets. This will be great in an article.)


old video https://www.youtube.com/watch?v=f20882ZSdkU
 old slides: 1474729847848977russellbrownbiggersetseuc2016.pdf

 Link it to DSON (find the paper trai)

 Is our RBILT on dots or items?! It needs to be on dots. RBILT needs fixed size elements.


## 7th Oct, 2025
### 10h41
Ok, what if I just try and get a grasp on what all the code does now, and forget buffers and anti-entropy, and just see, if I can start 3 nodes and replicate data and use the redis-cli?

### TODOs
1. Actor ID [x]
2. Quick and easy dev.rs type way to start up three nodes
3. Monitoring/logging
4. Manual testing
5. Proptest against full state AddWins Set
6. benchmark
7. buffers
8. anti-entopy



### Claude suggestions fo replicationWrite Path (Node 1 executes SADD):
1. Apply locally, create Operation
2. Add to unacked_buffer[node2, node3]
3. Send to peers
4. Return OK to client

Receive Path (Node 2 receives operation):
1. Receive Operation
2. Send ACK immediately (receipt confirmed)  ← CHANGED
3. Check causality: context.dominates(local_vv)?
   - YES: Apply to DB, update VV, try pending buffer
   - NO: Add to pending_buffer (wait for missing ops)

Sender (Node 1 receives ACK):
1. Remove operation from unacked_buffer[node2]
2. Done with this op for this peer

Retry Path (Node 1 background task):
1. Check unacked_buffer for operations > ack_timeout_ms
2. Resend if no ACK received (peer might be down/message lost)
3. Give up after max_retries (rely on anti-entropy)

Anti-Entropy (Periodic, all nodes):
1. Exchange version vectors with peers
2. Detect gaps (missing dots)
3. Use RIBLT to efficiently find/exchange missing operations
4. Repairs:
   - Operations lost in network (never received)
   - Operations stuck in pending buffer (unblock causal chain)
   - Operations where ACK was lost (sender kept retrying unnecessarily)
(if we exchange vv's to find missing dots, do we even need RBILT? I thought RBILT was to be run over the set of digest dots!)

### 12h29
We're going to need the same versioned data with atomic visibility hack here too, or large updates will block the user

## 14/10/2025
### 14h17
TODO:
- [ ] In replicated_add|remove of storage take the remote version vector and prune dots that way (if we're sure we're enforcing causal consistency)
