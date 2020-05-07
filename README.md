# Fault-tolerant Distributed Queue with server failures implemented with Sequencer based Protocol

Implemenation of faul tolerant and distributed queue with using sequencer based protocol.
Detailed write up [link](https://github.com/prashanth-thipparthi/fault_tolerant_queue_with_server_failures/blob/master/ProgrammingAssignment2.pdf "Title").

# Compiling code
mvn clean install.

# Running Program.
## Run the servers using the below command by changing port numers
mvn exec:java@follower -Dexec.args="<port_number>" &
## Run the client using the following command.
mvn exec:java@client

## How it works.
1. We have to run the servers first with 5 different port numbers as arguments.

2. Next we have to run the client and select the server to connect to.

3. Perform the operations on the queue.

4. System can tolerate server failures.

5. When a server restarts or comes back from crash failure and joins the cluster, messages are replayed and consistency is maintained.

6. Verify the queue by connecting to a different server apart from the first server.
