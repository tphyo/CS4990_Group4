
# CS 4990 Project

from mpi4py import MPI
import numpy as np

# returns shortest path from one node to all other nodes
def dijkstras(G, i):
    pass

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

    closeness_centrality(G, n, p)

    # send results back to processor 0

    # processor 0 prints closeness centrality for all nodes to output.txt

    # processor 0 prints nodes with top 5 centrality values to output.txt

    # processor 0 prints average of all nodes' centrality values to output.txt
