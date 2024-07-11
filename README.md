### Kademlia
Kademlia is a resilient and tolerant distributed hash table and network implementation. A lot of the tolerance and resilience depend on implementation choices.
##
Read more here: https://medium.com/@vmandke/kademlia-89142a8c2627
##

### Running and starting the peers
#### Starting the network
```
python3 server.py
```
#### Join using the bootstrap node
```
python3 server.py --bid "1111" --port 4244 --bootstrap-bid "0000" --bootstrap-ip "0.0.0.0" --bootstrap-port "4242"
python3 server.py --bid "1101" --port 4246 --bootstrap-bid "0000" --bootstrap-ip "0.0.0.0" --bootstrap-port "4242"
```
