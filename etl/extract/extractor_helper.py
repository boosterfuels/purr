from multiprocessing import cpu_count
from etl.monitor import logger
import os
from etl.load import init_pg as postgres
from multiprocessing import Process
from etl.extract import transfer_info
from etl.extract import collection
import time
from etl.transform import config_parser as cp


name_extra_props_pg = "_extra_props"
name_extra_props_mdb = "extraProps"


CURR_FILE = '[EXTRACTOR_HELPER]'


def get_docs(mdb, extra_props, coll_name, attr_details, new_columns):
    if extra_props is True:
        docs = collection.get_by_name(mdb, coll_name)
    elif len(new_columns) > 0:
        print("NEW columns!")
    else:
        attr_source = [k for k, v in attr_details.items()]
        docs = collection.get_by_name_reduced(mdb, coll_name, attr_source)
    return docs


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
                   has_extra_props,
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
                has_extra_props,
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


def work(transferring, attr_details, has_extra_props, r):
    try:
        r.insert(
            transferring,
            attr_details,
            has_extra_props
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


# schema changes
def prepare_attr_details(
        attrs_cm,
        attrs_mdb,
        types_cm,
        extra_props,
        type_x_props_pg=None):
    '''
    Adds extra properties field to the attribute details:
    (attr_details).
    Extra properties are not part of the original document
    and they need to be added in this separate step.
    Returns
    -------
    attr_details : list
                : details for mapping each field
                - name of the field (in mongodb)
                - name in the collection map (for pg)
                - type in the collection map (for pg)
                - default value

    Parameters
    ----------
    attrs_cm : list
                : attribute names from config file
    attrs_mdb : list
                : field names of MongoDB document
    types_cm : list
                : types from config files
    extra_props_type : string
                : type of the extra property
    Example
    -------
    attrs_new = [kit_cat, birdy_bird]
    attrs_original = [kitCat, birdyBird]
    types = ['text', 'text']
    extra_props_type = 'jsonb'
    res = append_extra_props(
        attrs_new, attrs_original, types, extra_props_type
    )
    '''
    if extra_props is True:

        attrs_cm.append(name_extra_props_pg)
        attrs_mdb.append(name_extra_props_mdb)
        types_cm.append(type_x_props_pg)

    attr_details = {}
    for i in range(0, len(attrs_mdb)):
        details = {}
        details["name_cm"] = attrs_cm[i]
        details["type_cm"] = types_cm[i]
        details["value"] = None
        attr_details[attrs_mdb[i]] = details
    return attr_details


def get_attr_details(coll_def, coll, has_extra_props):
    (
        att_new,
        att_orig,
        types,
        type_x_props_pg
    ) = cp.config_fields(coll_def, coll)

    # # TODO: check if this is necessary:
    # if types == []:
    #     return

    # Adding extra properties to inserted/updated row is necessary
    # because this attribute is not part of the original document
    # and anything that is not defined in the collection.yml file
    # will be pushed in this value. This function will also create
    # a dictionary which will contain all the information about
    # the attribute before and after the conversion.

    attr_details = prepare_attr_details(
        att_new, att_orig, types, has_extra_props, type_x_props_pg)
    return attr_details


def handle_failed_type_update(rel, attr_details):
    """
    Handles failed type conversion.
    Tries to update the schema. If not successful, prints a warning.
    TODO: rename old column and create a new column with the updated type
    """
    failed = rel.update_schema(attr_details)
    if failed is not None:
        for tuf in failed:
            name_pg = tuf[0]
            name_mdb = [
                attr for attr in attr_details if attr_details[attr]["name_cm"] == name_pg][0]
            type_orig = tuf[1].lower()
            type_new = attr_details[name_mdb]["type_cm"].lower()
            attr_details[name_mdb]["type_cm"] = type_orig
            logger.warn("""
                %s Type conversion is not possible for column '%s'.
                Skipping conversion %s -> %s.""" %
                        (
                            CURR_FILE,
                            name_pg, type_orig, type_new))

    return attr_details


def add_extra_props(attrs_original, attrs_new, types, has_extra_props):
    """
    add extra properties when transferring data
    """

    # This dict contains all the necessary information about the
    # Mongo fields, Postgres columns and their types
    attr_details = {}
    attrs_mdb = attrs_original
    attrs_cm = attrs_new
    types_cm = types

    if has_extra_props is True:
        attrs_cm.append(name_extra_props_pg)
        types_cm.append(types)
        attrs_mdb.append(name_extra_props_mdb)

    for i in range(len(attrs_mdb)):
        details = {}
        details["name_cm"] = attrs_cm[i]
        details["type_cm"] = types[i]
        details["value"] = None
        attr_details[attrs_mdb[i]] = details
    return attr_details
