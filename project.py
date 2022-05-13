
# CS 4990 Project

from mpi4py import MPI
import numpy as np
from queue import PriorityQueue

# returns shortest path from one node to all other nodes
def dijkstra(G, start, goal):
    """ Uniform-cost search / dijkstra """
    visited = set()
    cost = {start: 0}
    parent = {start: None}
    todo = PriorityQueue()
  
    todo.put((0, start))
    while todo:
        while not todo.empty():
            _, vertex = todo.get() # finds lowest cost vertex
            # loop until we get a fresh vertex
            if vertex not in visited: 
                break
        else: # if todo ran out
            break # quit main loop
        visited.add(vertex)
        if vertex == goal:
            break
        for neighbor, distance in G[vertex]:
            if neighbor in visited: 
                continue # skip these to save time
            old_cost = cost.get(neighbor, float('inf')) # default to infinity
            new_cost = cost[vertex] + distance
            if new_cost < old_cost:
                todo.put((new_cost, neighbor))
                cost[neighbor] = new_cost
                parent[neighbor] = vertex

    return parent

def closeness_centrality(G, n, p):
    rank = comm.get_rank()
    first_node = rank * (n // p)
    last_node = (rank + 1) * (n // p) - 1
    results = []
    count = 0

    for i in range(first_node, last_node):
        # calculate shortest paths for node i usind dijkstra's
        shortest_paths = dijkstras(G, i)

        # calculate closeness centrality for node i
        closeness_centrality = 0
        for j in range(n - 1):
            closeness_centrality += shortest_paths[j]
        closeness_centrality *= 1 / (n - 1)
        closeness_centrality = 1 / closeness_centrality

        results[count] = closeness_centrality
        count += 1

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    comm = MPI.COMM_WORLD
   
    # input from the network
    s = comm.recv()

    closeness_centrality(G, n, p)

    # send results back to processor 0 [array produced by closeness_centrality] back to processor 0
 
    comm.send(x, 0, tag=0)

    # processor 0 prints closeness centrality for all nodes to output.txt

    # processor 0 prints nodes with top 5 centrality values to output.txt

    # processor 0 prints average of all nodes' centrality values to output.txt
    
    
if __name__ == '__main__':
	file = open("output.txt", "w")
	total = 0
    if rank == 0:
    	file.write("Closeness Centrality of all nodes: ")
    	for i in range(len(closeness_centrality_array)):
    		total += closeness_centrality_array[i];
    		file.write(closeness_centrality_array[i])
    		file.write("\n")

    	average = total/len(closeness_centrality_array)
    	file.write("Top 5 Closeness Centrality: ")
    	file.write("\n")
    	top5 = Nmaxelements(closeness_centrality_array, 5)

    	for j in range(len(top5)):
    		file.write(top5[j])
    		file.write("\n")

    	file.write("Average of all nodes: ")
    	file.write("\n")
    	file.write(average)
    	file.write("\n")

