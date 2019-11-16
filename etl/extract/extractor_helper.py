from multiprocessing import cpu_count
from etl.monitor import logger
import os
from etl.load import init_pg as postgres
from multiprocessing import Process
from etl.extract import transfer_info
import time

CURR_FILE = '[EXTRACTOR_HELPER]'


def get_n_process(nr_of_docs, MAX_N_ROWS):
    """
    Returns number of processes
    """
    # suggested number of processes is n(cpu)-1
    max_n_processes = cpu_count() - 1
    # default case
    n_processes = max_n_processes

    # determine the maximum number of docs one process will transfer at once
    # prettier: math.ceil(MAX_NR_OF_TRANSFERRED/max_n_processes)

    tmp_s_chunk = MAX_N_ROWS / max_n_processes
    size_of_chunk = int(tmp_s_chunk) + (tmp_s_chunk > 0)

    # if the number of documents is less than the number of documents that
    # can be transferred at once, determine the number of processes necessary
    # to be created
    if nr_of_docs <= MAX_N_ROWS:
        for i in range(0, max_n_processes):
            if (i * size_of_chunk) > nr_of_docs:
                n_processes = i
                break

    logger.info("%s n(docs)=%s, size(chunk)=%s, n(proc)=%s" %
                (CURR_FILE, nr_of_docs, size_of_chunk, n_processes))

    return n_processes, size_of_chunk
    # return 1, MAX_N_ROWS


def init_processes(pg_conns,
                   chunks,
                   attr_details,
                   include_extra_props,
                   relations):
    """
    Initializes processes for collection transfer.
    ------
    Params
    pg_conns: list of connection objects, req: 1 per process
    chunks: data chunks (2D list)
    attr_details: details about the relation's attributes

    ------
    Returns 
    processes : Process[]
                : processes that will transfer one chunk of data 
    """
    processes = []

    for i in range(len(pg_conns)):
        p = Process(
            target=work,
            args=(
                chunks[i],
                attr_details,
                include_extra_props,
                relations[i]
            )
        )
        processes.append(p)
    return processes


def start_processes(processes):
    """
    Starts all the processes
    """
    for p in processes:
        p.start()


def finish_processes(processes):
    """
        Ensures that all the processes completed 
        before the main process does anything that 
        depends on the work of these processes.
    """
    for p in processes:
        p.join()


def work(transferring, attr_details, include_extra_props, r):
    try:
        r.insert(
            transferring,
            attr_details,
            include_extra_props
        )
    except Exception as ex:
        logger.error("""%s pid=%s Transfer unsuccessful. %s""" % (
            CURR_FILE, os.getpid(), ex)
        )


def init_connections(n_process, db_dest):
    connections_pg = []
    for i in range(0, n_process):
        conn = postgres.PgConnection(
            db_dest
        )
        connections_pg.append(conn)
    return connections_pg


def close_connections(connections_pg):
    for conn in connections_pg:
        conn.__del__()


def init_chunks(n):
    chunks = []
    for i in range(0, n):
        chunks.append([])
    return chunks


def get_transfer_details(tables_empty, transfer_start, transfer_end):
    if tables_empty:
        action = 'INSERT'
    else:
        action = 'UPSERT'
    transfer_details = [action, transfer_start, transfer_end]
    return transfer_details


def get_vacuum_details(relation):
    vacuum_start = time.time()
    relation.vacuum()
    vacuum_end = time.time()
    vacuum_info = ['FULL VACUUM', vacuum_start, vacuum_end]
    return vacuum_info


def log(relation, n_docs, actions):
    log_entries = []
    for action in actions:
        log_entries.append(tuple([action[0], relation.relation_name,
                                  n_docs, action[1], action[2]])
                           )
    transfer_info.log_stats(relation.db, relation.schema, log_entries)
