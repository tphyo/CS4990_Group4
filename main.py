
# CS 4990 Project

from mpi4py import MPI
import numpy as np
import networkx as nx
import heapdict
import time
# import warnings


def dijkstra(G, source):
    distance = [float('inf')] * G.number_of_nodes()
    distance[source] = 0
    # previous = [None] * G.number_of_nodes()
    priority_queue = heapdict.heapdict()    # priority queue dictionary with node as key and priority as value

    # add all nodes to priority queue with their distance from source node as the priority
    for node in G:
        priority_queue[int(node)] = distance[int(node)]

    while len(priority_queue) != 0:
        u = priority_queue.popitem()[0]     # u is the node with the least distance from source node
        for v in G.neighbors(str(u)):       # iterate over neighbors of node u
            if int(v) in priority_queue:         # check if the node v is in the priority queue
                alt = distance[u] + 1
                if alt < distance[int(v)]:
                    distance[int(v)] = alt
                    # previous[v] = u
                    priority_queue.__delitem__(int(v))
                    priority_queue[int(v)] = alt

    return distance


def closeness_centrality(G, n, p):
    rank = comm.Get_rank()
    first_node = rank * (n // p)
    last_node = (rank + 1) * (n // p) - 1
    results = []

    for i in range(first_node, last_node + 1):
        # calculate shortest paths for node i using dijkstra's
        shortest_paths = dijkstra(G, i)
        # print('shortest paths for node', i)
        # print(shortest_paths)

        # add up all shortest paths
        total_shortest_paths = 0
        for j in shortest_paths:
            total_shortest_paths += j

        # calculate closeness centrality for node i and add to results list
        closeness_centrality = 1 / (total_shortest_paths / (n - 1))
        results.append(closeness_centrality)

    return results

def N_max_elements(list, N):
    result_list = []

    for i in range(0, N):
        maximum = 0

        for j in range(len(list)):
            if list[j] > maximum:
                maximum = list[j]

        list.remove(maximum)
        result_list.append(maximum)

    return result_list

# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    start = time.time()
    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    rank = comm.Get_rank()

    pathname = 'facebook_combined.txt'

    # read in the edge list and create an directed graph
    G = nx.read_edgelist(path=pathname)

    # find closeness centrality
    closeness_results = closeness_centrality(G, G.number_of_nodes(), size)

    # send results back to processor 0 [array produced by closeness_centrality] back to processor 0
    if rank == 0:
        for i in range(1, size):
            closeness_results.extend(comm.recv(5000, tag=i))
    elif rank > 0:
        comm.send(closeness_results, 0, tag=rank)

    if rank == 0:
        file = open("output.txt", "w")
        total = 0

        # processor 0 prints closeness centrality for all nodes to output.txt
        file.write("Closeness Centrality of all nodes: \n")
        for i in range(len(closeness_results)):
            # print(i, ' ', closeness_results[i])
            total += closeness_results[i];
            file.write(str(closeness_results[i]))
            file.write("\n")
        file.write("\n")
        average = total / len(closeness_results)

        # processor 0 prints nodes with top 5 centrality values to output.txt
        file.write("Top 5 Closeness Centrality: ")
        file.write("\n")
        top5 = N_max_elements(closeness_results, 5)
        for j in range(len(top5)):
            file.write(str(top5[j]))
            file.write("\n")
        file.write("\n")

        # processor 0 prints average of all nodes' centrality values to output.txt
        file.write("Average of all nodes: ")
        file.write("\n")
        file.write(str(average))
        file.write("\n")

        end = time.time()

        print("execution time: " + str(end-start) + " seconds.")
