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
