
# CS 4990 Project

from mpi4py import MPI
import numpy as np
import networkx as nx
from queue import PriorityQueue
from collections import defaultdict
import heapq as heap

def dijkstra(G, start, goal):
    """ Uniform-cost search / dijkstra """
    visited = set()
    cost = {start: 0}
    parent = {start: None}
    todo = PriorityQueue()

    todo.put((0, start))
    while todo:
        while not todo.empty():
            _, vertex = todo.get()  # finds lowest cost vertex
            # loop until we get a fresh vertex
            if vertex not in visited:
                break
        else:  # if todo ran out
            break  # quit main loop
        visited.add(vertex)
        if vertex == goal:
            break
        for neighbor, distance in G.nodes:
            if neighbor in visited:
                continue  # skip these to save time
            old_cost = cost.get(neighbor, float('inf'))  # default to infinity
            new_cost = cost[vertex] + distance
            if new_cost < old_cost:
                todo.put((new_cost, neighbor))
                cost[neighbor] = new_cost
                parent[neighbor] = vertex

    return cost

def closeness_centrality(G, n, p):
    rank = comm.Get_rank()
    first_node = rank * (n // p)
    last_node = (rank + 1) * (n // p) - 1
    results = []
    count = 0

    for i in range(first_node, last_node):
        # calculate shortest paths for node i using dijkstra's
        for j in range(0, n):
            shortest_paths = dijkstra(G, i, j)

        # calculate closeness centrality for node i
        closeness_centrality = 0
        for j in range(n - 1):
            closeness_centrality += shortest_paths[j]
        closeness_centrality *= 1 / (n - 1)
        closeness_centrality = 1 / closeness_centrality

        results[count] = closeness_centrality
        count += 1

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
    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    rank = comm.Get_rank()

    pathname = 'facebook_combined.txt'

    # read in the edge list and create a directed graph
    G = nx.read_edgelist(path=pathname, create_using=nx.DiGraph)

    # find closeness centrality
    closeness_results = closeness_centrality(G, G.number_of_nodes(), size)

    # send results back to processor 0 [array produced by closeness_centrality] back to processor 0
    if rank == 0:
        result = []
        for i in range(1, size):
            result.append(comm.recv(i, tag=i))
    elif rank > 0:
        comm.send(closeness_results, 0, tag=rank)

    if rank == 0:
        file = open("output.txt", "w")
        total = 0

        # processor 0 prints closeness centrality for all nodes to output.txt
        file.write("Closeness Centrality of all nodes: ")
        for i in range(len(closeness_results)):
            total += closeness_results[i];
            file.write(closeness_results[i])
            file.write("\n")

        average = total / len(closeness_results)

        # processor 0 prints nodes with top 5 centrality values to output.txt
        file.write("Top 5 Closeness Centrality: ")
        file.write("\n")
        top5 = N_max_elements(closeness_results, 5)
        for j in range(len(top5)):
            file.write(top5[j])
            file.write("\n")

        # processor 0 prints average of all nodes' centrality values to output.txt
        file.write("Average of all nodes: ")
        file.write("\n")
        file.write(average)
        file.write("\n")
